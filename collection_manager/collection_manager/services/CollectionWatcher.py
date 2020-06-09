import logging
import os
from typing import List, Dict, Callable

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from yaml.scanner import ScannerError

from collection_manager.entities import Collection

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CollectionWatcher:
    def __init__(self, collections_path: str,
                 collection_updated_callback: Callable[[Collection], any],
                 granule_updated_callback: Callable[[str, Collection], any]):
        self._collections_path = collections_path
        self._collection_updated = collection_updated_callback
        self._granule_updated = granule_updated_callback
        self._observer = Observer()
        self._watches = {}
        self._collections: Dict[str, Collection] = {}

    def start_watching(self):
        """
        Start observing filesystem events for added/modified granules or changes to the Collections configuration file.
        When an event occurs, call the appropriate callback that was passed in during instantiation.
        :return: None
        """
        self._observer.schedule(_CollectionEventHandler(file_path=self._collections_path, callback=self._refresh),
                                os.path.dirname(self._collections_path))
        self._refresh()
        self._observer.start()

    def collections(self) -> List[Collection]:
        """
        Return a list of all Collections being watched.
        :return: A list of Collections
        """
        return list(self._collections.values())

    def _load_collections(self):
        try:
            with open(self._collections_path, 'r') as f:
                collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            new_collections = {}
            for _, collection_dict in collections_yaml.items():
                collection = Collection.from_dict(collection_dict)
                directory = collection.directory()
                if directory == os.path.dirname(self._collections_path):
                    logger.error(f"Collection {collection.dataset_id} uses granule directory {collection.path} "
                                 f"which is the same directory as the collection configuration file, "
                                 f"{self._collections_path}. The granules need to be in their own directory. "
                                 f"Ignoring collection {collection.dataset_id} for now.")
                if directory in new_collections:
                    logger.error(f"Ingestion order {collection.dataset_id} uses granule directory {directory} "
                                 f"which conflicts with ingestion order {new_collections[directory].dataset_id}."
                                 f" Ignoring {collection.dataset_id}.")
                else:
                    new_collections[directory] = collection

            self._collections = new_collections
        except FileNotFoundError:
            logger.error(f"Collection configuration file not found at {self._collections}.")
        except yaml.scanner.ScannerError:
            logger.error(f"Bad YAML syntax in collection configuration file. Will attempt to reload collections "
                         f"after the next configuration change.")

    def _refresh(self):
        for collection in self._get_updated_collections():
            self._collection_updated(collection)

        self._unschedule_watches()
        self._schedule_watches()

    def _get_updated_collections(self) -> List[Collection]:
        old_collections = self.collections()
        self._load_collections()
        return list(set(self.collections()) - set(old_collections))

    def _unschedule_watches(self):
        for directory, watch in self._watches.items():
            self._observer.unschedule(watch)
        self._watches.clear()

    def _schedule_watches(self):
        for collection in self.collections():
            granule_event_handler = _GranuleEventHandler(self._granule_updated, collection)
            directory = collection.directory()
            if directory not in self._watches:
                self._watches[directory] = self._observer.schedule(granule_event_handler, directory)


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

    def __init__(self, granule_updated: Callable[[str, Collection], any], collection: Collection):
        self._granule_updated = granule_updated
        self._collection = collection

    def on_created(self, event):
        super().on_created(event)
        if self._collection.owns_file(event.src_path):
            self._granule_updated(event.src_path, self._collection)
