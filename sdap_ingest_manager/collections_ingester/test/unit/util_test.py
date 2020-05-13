import unittest
import logging
from sdap_ingest_manager.collections_ingester import LocalConfiguration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MyTestCase(unittest.TestCase):
    def test_read_local_configuration(self):
        config = LocalConfiguration(config_path="sdap_ingest_manager/collections_ingester/resources/config").get()
        logger.info(f"interpolated value is {config['COLLECTIONS_YAML_CONFIG']['yaml_file']}")
        self.assertEqual(config['COLLECTIONS_YAML_CONFIG']['yaml_file'],
                         "sdap_ingest_manager/collections_ingester/resources/config/collections.yml")


if __name__ == '__main__':
    unittest.main()
