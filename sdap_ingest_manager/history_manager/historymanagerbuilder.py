import logging
import sdap_ingest_manager.history_manager.datasetingestionhistoryfile
import sdap_ingest_manager.history_manager.datasetingestionhistorysolr
from sdap_ingest_manager.history_manager import DatasetIngestionHistorySolrException


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DatasetIngestionHistoryBuilder:

    _HISTORY_MANAGER_OBJECT_SUPPORTED = {'DatasetIngestionHistoryFile', 'DatasetIngestionHistorySolr'}

    def __init__(self, history_manager=None, history_path = None, solr_url=None):

        self._history_manager_object = history_manager
        logger.info(f"Initialize {history_manager} builder")
        self._HISTORY_MANAGER_MODULE = self.__module__[0:self.__module__.rfind('.')]
        if history_manager == '.'.join([self._HISTORY_MANAGER_MODULE, 'DatasetIngestionHistoryFile']):
            if history_path:
                self._history_path = history_path
            else:
                logger.error(f"mandatory history_path option is missing")
        elif history_manager == '.'.join([self._HISTORY_MANAGER_MODULE, 'DatasetIngestionHistorySolr']):
            if solr_url:
                self._solr_url = solr_url
            else:
                logger.error(f"mandatory solr_url option is missing")
        else:
            logger.error(f"history manager {history_manager} is not supported")
            logger.info(f"supported history managers are {self._HISTORY_MANAGER_OBJECT_SUPPORTED}")

    def get_history_manager(self, dataset_id, signature_fun):
        if self._history_manager_object == '.'.join([self._HISTORY_MANAGER_MODULE, 'DatasetIngestionHistoryFile']):
            return eval(self._history_manager_object)(self._history_path, dataset_id, signature_fun)
        elif self._history_manager_object == '.'.join([self._HISTORY_MANAGER_MODULE, 'DatasetIngestionHistorySolr']):
            try:
                return eval(self._history_manager_object)(self._solr_url, dataset_id, signature_fun)
            except DatasetIngestionHistorySolrException:
                return None
        else:
            return None
