import logging
import os
from typing import List, Dict, Callable

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from yaml.scanner import ScannerError

from collection_manager import Collection

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

    def _load_orders(self):
        try:
            with open(self._collections_path, 'r') as f:
                orders_yml = yaml.load(f, Loader=yaml.FullLoader)
            new_ingestion_orders = {}
            for _, ingestion_order in orders_yml.items():
                new_order = Collection.from_dict(ingestion_order)
                directory = new_order.directory()
                if directory == os.path.dirname(self._collections_path):
                    logger.error(f"Ingestion order {new_order.dataset_id} uses granule directory {new_order.path} "
                                 f"which is the same directory as the collections file, {self._collections_path}. The "
                                 f"collections file cannot share a directory with the granules. Ignoring ingestion "
                                 f"order {new_order.dataset_id} for now.")
                if directory in new_ingestion_orders:
                    logger.error(f"Ingestion order {new_order.dataset_id} uses granule directory {directory} "
                                 f"which conflicts with ingestion order {new_ingestion_orders[directory].dataset_id}."
                                 f" Ignoring {new_order.dataset_id}.")
                else:
                    new_ingestion_orders[directory] = new_order

            self._collections = new_ingestion_orders
        except FileNotFoundError:
            logger.error(f"Collection configuration file not found at {self._collections}.")
        except yaml.scanner.ScannerError:
            logger.error(f"Bad YAML syntax in collection configuration file. Will attempt to reload collections "
                         f"after the next configuration change.")

    def _refresh(self):
        for updated_order in self._get_updated_orders():
            self._collection_updated(updated_order)

        self._unschedule_watches()
        self._schedule_watches()

    def _get_updated_orders(self) -> List[Collection]:
        old_orders = self.collections()
        self._load_orders()
        return list(set(self.collections()) - set(old_orders))

    def _unschedule_watches(self):
        for directory, watch in self._watches.items():
            self._observer.unschedule(watch)
        self._watches.clear()

    def _schedule_watches(self):
        for ingestion_order in self.collections():
            granule_event_handler = _GranuleEventHandler(self._granule_updated, ingestion_order)
            directory = ingestion_order.directory()
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
    def __init__(self, granule_updated: Callable[[str, Collection], any], ingestion_order: Collection):
        self._granule_updated = granule_updated
        self._ingestion_order = ingestion_order

    def on_created(self, event):
        super().on_created(event)
        if self._ingestion_order.owns_file(event.src_path):
            self._granule_updated(event.src_path, self._ingestion_order)
