import logging
import os.path
from typing import Dict
import yaml

from collection_manager.entities import Collection
from collection_manager.services import MessagePublisher
from collection_manager.services.history_manager import IngestionHistory, GranuleStatus
from collection_manager.services.history_manager.IngestionHistory import IngestionHistoryBuilder

logger = logging.getLogger(__name__)

SUPPORTED_FILE_EXTENSIONS = ['.nc', '.h5']
MESSAGE_TEMPLATE = os.path.join(os.path.dirname(__file__), '../resources/dataset_config_template.yml')


class CollectionProcessor:

    def __init__(self, message_publisher: MessagePublisher, history_manager_builder: IngestionHistoryBuilder):
        self._publisher = message_publisher
        self._history_manager_builder = history_manager_builder
        self._history_manager_cache: Dict[str, IngestionHistory] = {}

        with open(MESSAGE_TEMPLATE, 'r') as config_template_file:
            self._config_template = config_template_file.read()

    async def process_collection(self, collection: Collection):
        """
        Given a Collection, detect new granules that need to be ingested and publish RabbitMQ messages for each.
        :param collection: A Collection definition
        :return: None
        """
        for granule in collection.files_owned():
            await self.process_granule(granule, collection)

    async def process_granule(self, granule: str, collection: Collection):
        """
        Determine whether a granule needs to be ingested, and if so publish a RabbitMQ message for it.
        :param granule: A path to a granule file
        :param collection: A Collection against which to evaluate the granule
        :return: None
        """
        if not self._file_supported(granule):
            return

        history_manager = self._get_history_manager(collection.dataset_id)
        granule_status = await history_manager.get_granule_status(granule, collection.date_from, collection.date_to)

        if granule_status is GranuleStatus.DESIRED_FORWARD_PROCESSING:
            logger.info(f"New granule '{granule}' detected for forward-processing ingestion "
                        f"in collection '{collection.dataset_id}'.")
            if collection.forward_processing_priority is not None:
                use_priority = collection.forward_processing_priority
            else:
                use_priority = collection.historical_priority
        elif granule_status is GranuleStatus.DESIRED_HISTORICAL:
            logger.info(f"New granule '{granule}' detected for historical ingestion in collection "
                        f"'{collection.dataset_id}'.")
            use_priority = collection.historical_priority
        else:
            logger.debug(f"Granule '{granule}' detected but has already been ingested in "
                         f"collection '{collection.dataset_id}'. Skipping.")
            return

        dataset_config = self._fill_template(granule, collection)
        await self._publisher.publish_message(body=dataset_config, priority=use_priority)
        await history_manager.push(granule)

    @staticmethod
    def _file_supported(file_path: str):
        ext = os.path.splitext(file_path)[-1]
        return ext in SUPPORTED_FILE_EXTENSIONS

    def _get_history_manager(self, dataset_id: str) -> IngestionHistory:
        if dataset_id not in self._history_manager_cache:
            self._history_manager_cache[dataset_id] = self._history_manager_builder.build(dataset_id=dataset_id)
        return self._history_manager_cache[dataset_id]

    @staticmethod
    def _fill_template(granule_path: str, collection: Collection) -> str:
        config_dict = {
            'granule': {
                'resource': granule_path
            },
            'slicer': {
                'name': 'sliceFileByStepSize',
                'dimension_step_sizes': dict(collection.slices)
            },
            'processors': [
                {
                    'name': collection.projection,
                    **dict(collection.dimension_names),
                },
                {'name': 'emptyTileFilter'},
                {'name': 'kelvinToCelsius'},
                {
                    'name': 'tileSummary',
                    'dataset_name': collection.dataset_id
                },
                {'name': 'generateTileId'}
            ]
        }
        config_str = yaml.dump(config_dict)
        logger.debug(f"Templated dataset config:\n{config_str}")
        return config_str
