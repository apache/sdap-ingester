import logging
import os.path
from typing import Dict

import pystache

from collection_manager import Collection
from collection_manager import MessagePublisher
from collection_manager import IngestionHistory, GranuleStatus
from collection_manager import IngestionHistoryBuilder
from collection_manager import nfs_mount_parse

logger = logging.getLogger(__name__)

SUPPORTED_FILE_EXTENSIONS = ['.nc', '.h5']
MESSAGE_TEMPLATE = os.path.join(os.path.dirname(__file__), '../resources/dataset_config_template.yml')


class CollectionProcessor:

    def __init__(self,
                 message_publisher: MessagePublisher,
                 history_manager_builder: IngestionHistoryBuilder,
                 deconstruct_nfs=False):
        self._publisher = message_publisher
        self._history_manager_builder = history_manager_builder
        self._nfs_mount_points = nfs_mount_parse.get_nfs_mount_points() if deconstruct_nfs else None
        self._history_manager_cache: Dict[str, IngestionHistory] = {}

        with open(MESSAGE_TEMPLATE, 'r') as config_template_file:
            self._config_template = config_template_file.read()

    def process_collection(self, collection: Collection):
        """
        Given a Collection, detect new granules that need to be ingested and publish RabbitMQ messages for each.
        :param collection: A Collection definition
        :return: None
        """
        for granule in collection.files_owned():
            self.process_granule(granule, collection)

    def process_granule(self, granule: str, collection: Collection):
        """
        Determine whether a granule needs to be ingested, and if so publish a RabbitMQ message for it.
        :param granule: A path to a granule file
        :param collection: A Collection against which to evaluate the granule
        :return: None
        """
        if not self._file_supported(granule):
            return

        history_manager = self._get_history_manager(collection.dataset_id)
        granule_status = history_manager.get_granule_status(granule, collection.date_from, collection.date_to)

        if granule_status is GranuleStatus.DESIRED_FORWARD_PROCESSING:
            logger.info(f"New granule '{granule}' detected for forward-processing ingestion.")
            use_priority = collection.forward_processing_priority
        elif granule_status is GranuleStatus.DESIRED_HISTORICAL:
            logger.info(f"New granule '{granule}' detected for historical ingestion.")
            use_priority = collection.historical_priority
        else:
            logger.info(f"Granule '{granule}' detected but has already been ingested or is not in desired time range. "
                        f"Skipping.")
            return

        dataset_config = self._fill_template(collection,
                                             config_template=self._config_template,
                                             nfs_mount_points=self._nfs_mount_points)
        self._publisher.publish_message(dataset_config, use_priority)
        history_manager.push(granule)

    @staticmethod
    def _file_supported(file_path: str):
        ext = os.path.splitext(file_path)[-1]
        return ext in SUPPORTED_FILE_EXTENSIONS

    def _get_history_manager(self, dataset_id: str) -> IngestionHistory:
        if dataset_id not in self._history_manager_cache:
            self._history_manager_cache[dataset_id] = self._history_manager_builder.build(dataset_id=dataset_id)
        return self._history_manager_cache[dataset_id]

    @staticmethod
    def _fill_template(collection: Collection,
                       config_template: str,
                       nfs_mount_points=None) -> str:
        if nfs_mount_points:
            granule = nfs_mount_parse.replace_mount_point_with_service_path(collection.path, nfs_mount_points)
        else:
            granule = collection.path

        renderer = pystache.Renderer()
        config_content = renderer.render(config_template,
                                         {
                                             'granule': granule,
                                             'dataset_id': collection.dataset_id,
                                             'variable': collection.variable
                                         })
        logger.debug(f"Templated dataset config:\n{config_content}")
        return config_content
