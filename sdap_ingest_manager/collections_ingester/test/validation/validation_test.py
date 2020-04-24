import unittest

import sdap_ingest_manager.collections_ingester.util
from sdap_ingest_manager import collections_ingester
import logging
import filecmp
import os
import sys
from pathlib import Path


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

full_path = collections_ingester.full_path


class TestValidationMgr(unittest.TestCase):
    _config = collections_ingester.read_local_configuration()

    def setUp(self):
        logger.info("\n===== VALIDATION TESTS =====")
        super().setUp()
        self.sdap_ingest_manager_home = os.path.join(sys.prefix, '.sdap_ingest_manager')
        self.expected_dataset_configuration_file = os.path.join(Path(__file__).parent.absolute(),
                                                                "../data/dataset_config_file_ok.yml")
        self.granule_list_file_result = full_path("tmp/granule_lists/avhrr-oi-analysed-sst-granules.lst")
        self.dataset_config_file_result = full_path("tmp/dataset_config/avhrr-oi-analysed-sst-config.yml")
        self.job_deployment_file_path = full_path(self._config.get("INGEST", "job_deployment_template"))
        self.connection_settings_file_path = full_path(self._config.get("INGEST", "connection_config"))

    def test_validation_with_local_collection_configuration(self):
        logger.info("validation with local validation configuration file")

        def collection_row_callback(row):
            collections_ingester \
                .collection_row_callback(row,
                                         full_path(self._config.get("OPTIONS", "collection_config_template")),
                                         full_path(self._config.get("LOCAL_PATHS", "granule_file_list_path")),
                                         full_path(self._config.get("LOCAL_PATHS", "collection_config_path")),
                                         full_path(self._config.get("LOCAL_PATHS", "history_path")),
                                         deconstruct_nfs=False,
                                         job_deployment_template=self.job_deployment_file_path,
                                         connection_settings=self.connection_settings_file_path,
                                         profiles=self._config.get("INGEST", "connection_profile").split(
                                             ','),
                                         namespace=self._config.get("INGEST", "kubernetes_namespace"),
                                         dry_run=True
                                         )

        collections_ingester.read_yaml_collection_config(full_path(self._config.get('COLLECTIONS_YAML_CONFIG', 'yaml_file')),
                                                        collection_row_callback)

    @unittest.skip("does not work in github action environment")
    def test_validation_no_parse_nfs(self):
        logger.info("validation test without nfs parsing")

        def collection_row_callback_no_parse_nfs(row):
            collections_ingester \
                .collection_row_callback(row,
                                         full_path(self._config.get("OPTIONS", "collection_config_template")),
                                         full_path(self._config.get("LOCAL_PATHS", "granule_file_list_path")),
                                         full_path(self._config.get("LOCAL_PATHS", "collection_config_path")),
                                         full_path(self._config.get("LOCAL_PATHS", "history_path")),
                                         deconstruct_nfs=False,
                                         job_deployment_template=self.job_deployment_file_path,
                                         connection_settings=self.connection_settings_file_path,
                                         profiles=self._config.get("INGEST", "connection_profile").split(
                                             ','),
                                         namespace=self._config.get("INGEST", "kubernetes_namespace"),
                                         dry_run=True
                                         )

        self.validation_with_google_spreadsheet_and_callback(collection_row_callback_no_parse_nfs)

    @unittest.skip("does not work in github action environment")
    def test_validation_parse_nfs(self):
        logger.info("validation test with nfs parsing")

        def collection_row_callback_parse_nfs(row):
            collections_ingester \
                .collection_row_callback(row,
                                         full_path(self._config.get("OPTIONS", "collection_config_template")),
                                         full_path(self._config.get("LOCAL_PATHS", "granule_file_list_path")),
                                         full_path(self._config.get("LOCAL_PATHS", "collection_config_path")),
                                         full_path(self._config.get("LOCAL_PATHS", "history_path")),
                                         deconstruct_nfs=True,
                                         job_deployment_template=self.job_deployment_file_path,
                                         connection_settings=self.connection_settings_file_path,
                                         profiles=self._config.get("INGEST", "connection_profile").split(','),
                                         namespace=self._config.get("INGEST", "kubernetes_namespace"),
                                         dry_run=True)

        self.validation_with_google_spreadsheet_and_callback(collection_row_callback_parse_nfs)

    def validation_with_google_spreadsheet_and_callback(self, call_back):
        collections_ingester.read_google_spreadsheet(
            self._config.get('COLLECTIONS_GOOGLE_SPREADSHEET', 'scope'),
            self._config.get("COLLECTIONS_GOOGLE_SPREADSHEET", "spreadsheet_id"),
            self._config.get('COLLECTIONS_GOOGLE_SPREADSHEET', 'sheet_name'),
            self._config.get("COLLECTIONS_GOOGLE_SPREADSHEET", "cell_range"),
            call_back
        )
        self.result_validation()

    def validation_with_local_yaml_file_and_callback(self, call_back):
        collections_ingester.read_yaml_collection_config(
            self._config.get("COLLECTIONS_YAML_CONFIG", "yaml_file"),
            call_back
        )
        self.result_validation()

    def result_validation(self):
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
