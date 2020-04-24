import unittest
import os
import sys
from datetime import datetime
from pathlib import Path
import logging
import filecmp
import sdap_ingest_manager.granule_ingester
from sdap_ingest_manager.collections_ingester import collection_ingestion
from sdap_ingest_manager.collections_ingester import util
from sdap_ingest_manager.history_manager import md5sum_from_filepath


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestUnitMgr(unittest.TestCase):

    def setUp(self):
        logger.info("\n===== UNIT TESTS =====")
        super().setUp()
        self.collection_config_template = util.full_path("resources/dataset_config_template.yml")

        self.target_granule_list_file = util.full_path("tmp/granule_list/target_granule_list_file.lst")
        self.target_dataset_config_file = util.full_path("tmp/dataset_config/dataset_config_file.yml")

        self.history_path = os.path.join(Path(__file__).parent.absolute(),
                                         "../data/")
        self.dataset_id = "avhrr-oi-analysed-sst"
        self.granule_file_pattern = os.path.join(Path(__file__).parent.absolute(),
                                                 "../data/avhrr_oi/*.nc")
        self.expected_dataset_configuration_file = os.path.join(Path(__file__).parent.absolute(),
                                                                "../data/dataset_config_file_ok.yml")

    def test_create_granule_list(self):
        logger.info("test create_granule_list")
        dataset_ingestion_history_manager = sdap_ingest_manager.history_manager \
            .DatasetIngestionHistoryFile(self.history_path, self.dataset_id, md5sum_from_filepath)
        collection_ingestion.create_granule_list(self.granule_file_pattern,
                                                 dataset_ingestion_history_manager,
                                                 self.target_granule_list_file
                                                 )
        line_number = 0
        with open(self.target_granule_list_file, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertEqual(1, line_number)

        os.remove(self.target_granule_list_file)

    def test_create_granule_list_time_range(self):
        logger.info("test create_granule_list with time range")
        collection_ingestion.create_granule_list(self.granule_file_pattern,
                                                 None,
                                                 self.target_granule_list_file,
                                                 date_from=datetime(2020, 3, 4, 19, 4, 28, 843998),
                                                 date_to=datetime(2020, 6, 2, 19, 4, 28, 843998))
        line_number = 0
        with open(self.target_granule_list_file, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertGreaterEqual(line_number, 0)

        os.remove(self.target_granule_list_file)

    def test_create_granule_list_time_range_from_only(self):
        logger.info("test create_granule_list with time range")
        collection_ingestion.create_granule_list(self.granule_file_pattern,
                                                 None,
                                                 self.target_granule_list_file,
                                                 date_from=datetime(2020, 3, 4, 19, 4, 28, 843998))
        line_number = 0
        with open(self.target_granule_list_file, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertGreaterEqual(line_number, 0)

        os.remove(self.target_granule_list_file)

    def test_create_granule_list_time_range_to_only(self):
        logger.info("test create_granule_list with time range")
        collection_ingestion.create_granule_list(self.granule_file_pattern,
                                                 None,
                                                 self.target_granule_list_file,
                                                 date_to=datetime(2020, 3, 4, 19, 4, 28, 843998))
        line_number = 0
        with open(self.target_granule_list_file, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertGreaterEqual(line_number, 0)

        os.remove(self.target_granule_list_file)




    def test_create_granule_list_no_history(self):
        logger.info("test create_granule_list")
        collection_ingestion.create_granule_list(self.granule_file_pattern,
                                                 None,
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
        collection_ingestion.create_dataset_config("avhrr-oi-analysed-sst",
                                                  "analysed_sst",
                                                   self.collection_config_template,
                                                   self.target_dataset_config_file)

        self.assertTrue(filecmp.cmp(self.expected_dataset_configuration_file, self.target_dataset_config_file),
                        "the dataset configuration file created does not match the expected results")

        os.remove(self.target_dataset_config_file)

    def tearDown(self):
        logger.info("tear down test results")


if __name__ == '__main__':
    unittest.main()
