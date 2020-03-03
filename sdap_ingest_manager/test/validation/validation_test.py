import unittest
from sdap_ingest_manager import create_granule_list
import os
import logging
import filecmp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestValidationMgr(unittest.TestCase):

    def setUp(self):
        logger.info("\n===== VALIDATION TESTS =====")
        super().setUp()
        self.expected_dataset_configuration_file = "test/data/dataset_config_file_ok.yaml"
        self.granule_list_file_result = "tmp/granule_lists/AVHRR_OI_analysed_sst.lst"
        self.dataset_config_file_result = "tmp/dataset_config/AVHRR_OI_analysed_sst.yaml"
        self.test_tab = "VALIDATION_TAB_DO_NOT_TOUCH"

    def test_validation(self):
        logger.info("validation test")
        create_granule_list.read_google_spreadsheet(self.test_tab,
                                                    create_granule_list.collection_row_callback)
        # test the granule list file
        line_number = 0
        with open(self.granule_list_file_result, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertEqual(2, line_number)

        # test the configuration file
        self.assertTrue(filecmp.cmp(self.expected_dataset_configuration_file, self.dataset_config_file_result),
                        "the dataset configuration file created does not match the expected results")

    def tearDown(self):
        logger.info("tear down test results")
        os.remove(self.granule_list_file_result)
        #os.remove(self.dataset_config_file_result)


if __name__ == '__main__':
    unittest.main()
