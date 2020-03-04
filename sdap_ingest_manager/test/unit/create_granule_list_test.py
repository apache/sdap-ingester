import unittest
from sdap_ingest_manager import create_granule_list
import os
import logging
import filecmp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestUnitMgr(unittest.TestCase):

    def setUp(self):
        logger.info("\n===== UNIT TESTS =====")
        super().setUp()
        self.target_granule_list_file = "tmp/target_granule_list_file.lst"
        self.target_dataset_config_file = "tmp/dataset_config_file.yml"
        self.granule_file_pattern = "sdap_ingest_manager/test/data/avhrr_oi/*.nc"
        self.expected_dataset_configuration_file = "test/data/dataset_config_file_ok.yml"

    def test_create_granule_list(self):
        logger.info("test create_granule_list")
        create_granule_list.create_granule_list(self.granule_file_pattern,
                                                self.target_granule_list_file
                                                )
        line_number = 0
        with open(self.target_granule_list_file, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertEqual(2, line_number)

        os.remove(self.target_granule_list_file)

    def test_create_dataset_config(self):
        logger.info("test create_dataset_config")
        create_granule_list.create_dataset_config("avhrr-oi-analysed-sst",
                                                  "analysed_sst",
                                                  self.target_dataset_config_file)

        self.assertTrue(filecmp.cmp(self.expected_dataset_configuration_file, self.target_dataset_config_file),
                        "the dataset configuration file created does not match the expected results")

        os.remove(self.target_dataset_config_file)

    def tearDown(self):
        logger.info("tear down test results")


if __name__ == '__main__':
    unittest.main()
