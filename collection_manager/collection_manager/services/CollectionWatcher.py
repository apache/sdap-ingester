import logging
import os
from collections import defaultdict
from typing import List, Dict, Callable, Set

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from yaml.scanner import ScannerError

from collection_manager.entities import Collection

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CollectionWatcher:
    def __init__(self,
                 collections_path: str,
                 collection_updated_callback: Callable[[Collection], any],
                 granule_updated_callback: Callable[[str, Collection], any]):
        self._collections_path = collections_path
        self._collection_updated_callback = collection_updated_callback
        self._granule_updated_callback = granule_updated_callback
        
        self._collections_by_dir: Dict[str, Set[Collection]] = defaultdict(set)
        self._observer = Observer()

    def start_watching(self):
        """
        Start observing filesystem events for added/modified granules or changes to the Collections configuration file.
        When an event occurs, call the appropriate callback that was passed in during instantiation.
        :return: None
        """
        self._observer.schedule(_CollectionEventHandler(file_path=self._collections_path, callback=self._refresh),
                                os.path.dirname(self._collections_path))
        self._observer.start()
        self._refresh()

    def collections(self) -> List[Collection]:
        """
        Return a list of all Collections being watched.
        :return: A list of Collections
        """
        return [collection for collections in self._collections_by_dir.values() for collection in collections]

    def _load_collections(self):
        try:
            with open(self._collections_path, 'r') as f:
                collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            self._collections_by_dir.clear()
            for _, collection_dict in collections_yaml.items():
                collection = Collection.from_dict(collection_dict)
                directory = collection.directory()
                if directory == os.path.dirname(self._collections_path):
                    logger.error(f"Collection {collection.dataset_id} uses granule directory {collection.path} "
                                 f"which is the same directory as the collection configuration file, "
                                 f"{self._collections_path}. The granules need to be in their own directory. "
                                 f"Ignoring collection {collection.dataset_id} for now.")
                else:
                    self._collections_by_dir[directory].add(collection)

        except FileNotFoundError:
            logger.error(f"Collection configuration file not found at {self._collections_path}.")
        except yaml.scanner.ScannerError:
            logger.error(f"Bad YAML syntax in collection configuration file. Will attempt to reload collections "
                         f"after the next configuration change.")

    def _refresh(self):
        for collection in self._get_updated_collections():
            self._collection_updated_callback(collection)

        self._observer.unschedule_all()
        self._schedule_watches()

    def _get_updated_collections(self) -> List[Collection]:
        old_collections = self.collections()
        self._load_collections()
        return list(set(self.collections()) - set(old_collections))

    def _schedule_watches(self):
        for directory, collections in self._collections_by_dir.items():
            granule_event_handler = _GranuleEventHandler(self._granule_updated_callback, collections)
            # Note: the Watchdog library does not schedule a new watch
            # if one is already scheduled for the same directory
            self._observer.schedule(granule_event_handler, directory)


class _CollectionEventHandler(FileSystemEventHandler):
    """
    EventHandler that watches for changes to the Collections config file.
    """

    def __init__(self, file_path: str, callback: Callable[[], any]):
        self._callback = callback
        self._file_path = file_path

    def on_modified(self, event):
        super().on_modified(event)
        if event.src_path == self._file_path:
            self._callback()


class _GranuleEventHandler(FileSystemEventHandler):
    """
    EventHandler that watches for new or modified granule files.
    """

    def __init__(self, callback: Callable[[str, Collection], any], collections_for_dir: Set[Collection]):
        self._callback = callback
        self._collections_for_dir = collections_for_dir

    def on_created(self, event):
        super().on_created(event)
        for collection in self._collections_for_dir:
            if collection.owns_file(event.src_path):
                self._callback(event.src_path, collection)
