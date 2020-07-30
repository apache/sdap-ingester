import asyncio
import logging
import os
from collections import defaultdict
from typing import Dict, Callable, Set, Optional, Awaitable
import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from yaml.scanner import ScannerError

from collection_manager.entities import Collection
from collection_manager.entities.exceptions import RelativePathError, CollectionConfigParsingError, \
    CollectionConfigFileNotFoundError, MissingValueCollectionError, ConflictingPathCollectionError, \
    RelativePathCollectionError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CollectionWatcher:
    def __init__(self,
                 collections_path: str,
                 collection_updated_callback: Callable[[Collection], Awaitable],
                 granule_updated_callback: Callable[[str, Collection], Awaitable],
                 collections_refresh_interval: float = 30):
        if not os.path.isabs(collections_path):
            raise RelativePathError("Collections config  path must be an absolute path.")

        self._collections_path = collections_path
        self._collection_updated_callback = collection_updated_callback
        self._granule_updated_callback = granule_updated_callback
        self._collections_refresh_interval = collections_refresh_interval

        self._collections_by_dir: Dict[str, Set[Collection]] = defaultdict(set)
        self._observer = Observer()

        self._granule_watches = set()

    async def start_watching(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Periodically load the Collections Configuration file to check for changes,
        and observe filesystem events for added/modified granules. When an event occurs,
        call the appropriate callback that was passed in during instantiation.
        :return: None
        """

        await self._run_periodically(loop=loop,
                                     wait_time=self._collections_refresh_interval,
                                     coro=self._reload_and_reschedule())
        self._observer.start()

    def collections(self) -> Set[Collection]:
        """
        Return a set of all Collections being watched.
        :return: A set of Collections
        """
        return {collection for collections in self._collections_by_dir.values() for collection in collections}

    def _validate_collection(self, collection: Collection):
        directory = collection.directory()
        if not os.path.isabs(directory):
            raise RelativePathCollectionError(collection=collection)
        if directory == os.path.dirname(self._collections_path):
            raise ConflictingPathCollectionError(collection=collection)

    def _load_collections(self):
        try:
            with open(self._collections_path, 'r') as f:
                collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            self._collections_by_dir.clear()
            for collection_dict in collections_yaml['collections']:
                try:
                    collection = Collection.from_dict(collection_dict)
                    self._validate_collection(collection)
                    self._collections_by_dir[collection.directory()].add(collection)
                except MissingValueCollectionError as e:
                    logger.error(f"A collection is missing '{e.missing_value}'. Ignoring this collection for now.")
                except RelativePathCollectionError as e:
                    logger.error(f"Relative paths are not allowed for the 'path' property of a collection. "
                                 f"Ignoring collection '{e.collection.dataset_id}' until its path is fixed.")
                except ConflictingPathCollectionError as e:
                    logger.error(f"Collection '{e.collection.dataset_id}' has granule path '{e.collection.path}' "
                                 f"which uses same directory as the collection configuration file, "
                                 f"'{self._collections_path}'. The granules need to be in their own directory. "
                                 f"Ignoring collection '{e.collection.dataset_id}' for now.")
        except FileNotFoundError:
            raise CollectionConfigFileNotFoundError("The collection config file could not be found at "
                                                    f"{self._collections_path}")
        except yaml.scanner.ScannerError:
            raise CollectionConfigParsingError("Bad YAML syntax in collection configuration file. Will attempt "
                                               "to reload collections after the next configuration change.")
        except KeyError:
            raise CollectionConfigParsingError("The collections configuration YAML file does not conform to the "
                                               "proper schema. Will attempt to reload collections config after the "
                                               "next file modification.")

    def _get_updated_collections(self) -> Set[Collection]:
        old_collections = self.collections()
        self._load_collections()
        return self.collections() - old_collections

    async def _reload_and_reschedule(self):
        try:
            updated_collections = self._get_updated_collections()
            for collection in updated_collections:
                await self._collection_updated_callback(collection)
            if len(updated_collections) > 0:
                self._unschedule_watches()
                self._schedule_watches()
        except CollectionConfigParsingError as e:
            logger.error(e)

    def _unschedule_watches(self):
        for watch in self._granule_watches:
            self._observer.unschedule(watch)
        self._granule_watches.clear()

    def _schedule_watches(self):
        for directory, collections in self._collections_by_dir.items():
            granule_event_handler = _GranuleEventHandler(asyncio.get_running_loop(),
                                                         self._granule_updated_callback,
                                                         collections)
            # Note: the Watchdog library does not schedule a new watch
            # if one is already scheduled for the same directory
            try:
                self._granule_watches.add(self._observer.schedule(granule_event_handler, directory))
            except (FileNotFoundError, NotADirectoryError):
                bad_collection_names = ' and '.join([col.dataset_id for col in collections])
                logger.error(f"Granule directory {directory} does not exist. Ignoring {bad_collection_names}.")

    @classmethod
    async def _run_periodically(cls,
                                loop: Optional[asyncio.AbstractEventLoop],
                                wait_time: float,
                                coro: Awaitable):
        """
        Call a function periodically. This uses asyncio, and is non-blocking.
        :param loop: An optional event loop to use. If None, the current running event loop will be used.
        :param wait_time: seconds to wait between iterations of func
        :param coro: the coroutine that will be awaited
        :param args: any args that need to be provided to func
        """
        if loop is None:
            loop = asyncio.get_running_loop()
        await coro
        loop.call_later(wait_time, loop.create_task, cls._run_periodically(loop, wait_time, coro))


class _GranuleEventHandler(FileSystemEventHandler):
    """
    EventHandler that watches for new or modified granule files.
    """

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 callback: Callable[[str, Collection], Awaitable],
                 collections_for_dir: Set[Collection]):
        self._loop = loop
        self._callback = callback
        self._collections_for_dir = collections_for_dir

    def on_created(self, event):
        super().on_created(event)
        for collection in self._collections_for_dir:
            if collection.owns_file(event.src_path):
                self._loop.create_task(self._callback(event.src_path, collection))

    def on_modified(self, event):
        super().on_modified(event)
        if os.path.isdir(event.src_path):
            return

        for collection in self._collections_for_dir:
            if collection.owns_file(event.src_path):
                self._loop.create_task(self._callback(event.src_path, collection))
