import unittest
import os
import sys
import pathlib
from collection_manager.collection_manager.services.history_manager import FileIngestionHistory
from collection_manager.collection_manager.services.history_manager import md5sum_from_filepath

HISTORY_ROOT_PATH = os.path.join(sys.prefix,
                                 ".collection_manager",
                                 "tmp/history")
DATASET_ID = "zobi_la_mouche"


class DatasetIngestionHistoryFileTestCase(unittest.TestCase):
    ingestion_history = None

    # @unittest.skip("does not work without a solr server for history_manager")
    def test_get_md5sum(self):
        self.ingestion_history = FileIngestionHistory(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        self.ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
        result = self.ingestion_history._get_signature("blue")
        self.assertEqual(result, "12weeukrhbwerqu7wier")

    # @unittest.skip("does not work without a solr server for history_manager")
    def test_get_missing_md5sum(self):
        self.ingestion_history = FileIngestionHistory(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        self.ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
        result = self.ingestion_history._get_signature("green")
        self.assertEqual(result, None)

    def test_has_valid_cache(self):
        self.ingestion_history = FileIngestionHistory(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        # history_manager with this file
        current_file_path = pathlib.Path(__file__)
        self.ingestion_history.push(str(current_file_path))
        self.assertEqual(self.ingestion_history.already_ingested(str(current_file_path)), True)

    def test_has_valid_cache_with_latest_modifcation_signature(self):
        self.ingestion_history = FileIngestionHistory(HISTORY_ROOT_PATH, DATASET_ID, os.path.getmtime)
        # history_manager with this file
        current_file_path = pathlib.Path(__file__)
        self.ingestion_history.push(str(current_file_path))
        self.assertEqual(self.ingestion_history.already_ingested(str(current_file_path)), True)

    def test_has_not_valid_cache(self):
        self.ingestion_history = FileIngestionHistory(HISTORY_ROOT_PATH, DATASET_ID, md5sum_from_filepath)
        # history_manager with this file
        current_file_path = pathlib.Path(__file__)
        self.assertEqual(self.ingestion_history.already_ingested(str(current_file_path)), False)

    @unittest.skip("skip before history_manager dataset is not available")
    def test_purge(self):
        self.ingestion_history = FileIngestionHistory("/Users/loubrieu/PycharmProjects/collection_manager/venv/.collection_manager/tmp/history/",
                                                             "avhrr-oi-analysed-sst-toto",
                                                      lambda x : str(os.path.getmtime(x)))
        self.ingestion_history.purge()

    def tearDown(self):
        self.ingestion_history.reset_cache()
        del self.ingestion_history


if __name__ == '__main__':
    unittest.main()
