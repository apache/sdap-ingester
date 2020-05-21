import unittest
import logging
from sdap_ingest_manager.collections_ingester import LocalConfiguration
from sdap_ingest_manager.collections_ingester.util import ConfigWithPath

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MyTestCase(unittest.TestCase):
    def test_read_local_configuration(self):
        config = LocalConfiguration(config_path="sdap_ingest_manager/collections_ingester/resources/config").get()
        logger.info(f"interpolated value is {config['COLLECTIONS_YAML_CONFIG']['yaml_file']}")
        self.assertEqual(config['COLLECTIONS_YAML_CONFIG']['yaml_file'],
                         "sdap_ingest_manager/collections_ingester/resources/config/collections.yml")


    def test_config_with_path(self):
        config_with_path = ConfigWithPath()

        config_with_path.read(['sdap_ingest_manager/collections_ingester/resources/config/sdap_ingest_manager.ini.default'])
        logger.info(config_with_path.get('INGESTION_ORDER_YAML_CONFIG', 'yaml_file'))
        logger.info(config_with_path.get('GIT_CONFIG', 'git_local_dir'))




if __name__ == '__main__':
    unittest.main()
