import unittest
import os
import sys
import pathlib
from sdap_ingest_manager.history_manager import DatasetIngestionHistoryFile
from sdap_ingest_manager.history_manager import md5sum_from_filepath

HISTORY_ROOT_PATH = os.path.join(sys.prefix,
                                 ".sdap_ingest_manager",
                                 "tmp/history")
DATASET_ID = "zobi_la_mouche"


class DatasetIngestionHistoryFileTestCase(unittest.TestCase):
    ingestion_history = None

    # @unittest.skip("does not work without a solr server for test")
    def test_get_md5sum(self):
        self.ingestion_history = DatasetIngestionHistoryFile(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        self.ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
        result = self.ingestion_history._get_md5sum("blue")
        self.assertEqual(result, "12weeukrhbwerqu7wier")
        #self.ingestion_history.reset_cache()

    # @unittest.skip("does not work without a solr server for test")
    def test_get_missing_md5sum(self):
        self.ingestion_history = DatasetIngestionHistoryFile(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        self.ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
        result = self.ingestion_history._get_md5sum("green")
        self.assertEqual(result, None)
        #self.ingestion_history.reset_cache()

    def test_has_valid_cache(self):
        self.ingestion_history = DatasetIngestionHistoryFile(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        # test with this file
        current_file_path = pathlib.Path(__file__)
        self.ingestion_history.push(str(current_file_path))
        self.assertEqual(self.ingestion_history.has_valid_cache(str(current_file_path)), True)
        #self.ingestion_history.reset_cache()

    def test_has_not_valid_cache(self):
        self.ingestion_history = DatasetIngestionHistoryFile(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        # test with this file
        current_file_path = pathlib.Path(__file__)
        self.assertEqual(self.ingestion_history.has_valid_cache(str(current_file_path)), False)
        #self.ingestion_history.reset_cache()

    def tearDown(self):
        self.ingestion_history.reset_cache()
        del self.ingestion_history


if __name__ == '__main__':
    unittest.main()
