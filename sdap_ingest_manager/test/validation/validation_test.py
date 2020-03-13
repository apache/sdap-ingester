import unittest
from sdap_ingest_manager import create_granule_list
import logging
import filecmp
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestValidationMgr(unittest.TestCase):
    _config = create_granule_list.read_local_configuration()

    def setUp(self):
        logger.info("\n===== VALIDATION TESTS =====")
        super().setUp()

        self.expected_dataset_configuration_file = "test/data/dataset_config_file_ok.yml"
        self.granule_list_file_result = "tmp/granule_lists/avhrr-oi-analysed-sst-granules.lst"
        self.dataset_config_file_result = "tmp/dataset_config/avhrr-oi-analysed-sst-config.yml"

    def test_validation_no_parse_nfs(self):
        logger.info("validation test without nfs parsing")

        def collection_row_callback_no_parse_nfs(row):
            create_granule_list.collection_row_callback(row,
                                                        self._config.get("OPTIONS", "collection_config_template"),
                                                        self._config.get("LOCAL_PATHS", "granule_file_list_path"),
                                                        self._config.get("LOCAL_PATHS", "collection_config_path"),
                                                        self._config.get("LOCAL_PATHS", "log_path"),
                                                        self._config.get("INGEST", "job_deployment_template"),
                                                        self._config.get("INGEST", "connection_config"),
                                                        self._config.get("INGEST", "connection_profile"),
                                                        self._config.get("INGEST", "kubernetes_namespace"),
                                                        deconstruct_nfs=False,
                                                        dry_run=True)

        self.validation_with_callback(collection_row_callback_no_parse_nfs)

    def test_validation_parse_nfs(self):
        logger.info("validation test with nfs parsing")

        def collection_row_callback_parse_nfs(row):
            create_granule_list.collection_row_callback(row,
                                                        self._config.get("OPTIONS", "collection_config_template"),
                                                        self._config.get("LOCAL_PATHS", "granule_file_list_path"),
                                                        self._config.get("LOCAL_PATHS", "collection_config_path"),
                                                        self._config.get("LOCAL_PATHS", "log_path"),
                                                        self._config.get("INGEST", "job_deployment_template"),
                                                        self._config.get("INGEST", "connection_config"),
                                                        self._config.get("INGEST", "connection_profile"),
                                                        self._config.get("INGEST", "kubernetes_namespace"),
                                                        deconstruct_nfs=True, dry_run=True)

        self.validation_with_callback(collection_row_callback_parse_nfs)

    def validation_with_callback(self, call_back):
        create_granule_list.read_google_spreadsheet(self._config.get('COLLECTIONS_GOOGLE_SPREADSHEET', 'scope'),
                                                    self._config.get("COLLECTIONS_GOOGLE_SPREADSHEET",
                                                                     "spreadsheet_id"),
                                                    self._config.get('COLLECTIONS_GOOGLE_SPREADSHEET', 'sheet_name'),
                                                    self._config.get("COLLECTIONS_GOOGLE_SPREADSHEET", "cell_range"),
                                                    call_back
                                                    )
        # test the granule list file
        line_number = 0
        with open(self.granule_list_file_result, 'r') as f:
            for _ in f:
                line_number += 1

        self.assertEqual(2, line_number)

        # test the configuration file
        error_message = f'the dataset configuration file created does not match the expected results\n' \
                        'to compare run ' \
                        f'diff {self.expected_dataset_configuration_file} {self.dataset_config_file_result} '
        self.assertTrue(filecmp.cmp(self.expected_dataset_configuration_file, self.dataset_config_file_result),
                        error_message)

    def tearDown(self):
        logger.info("tear down test results")
        os.remove(self.granule_list_file_result)
        os.remove(self.dataset_config_file_result)


if __name__ == '__main__':
    unittest.main()
