import unittest
import os
import sys
from sdap_ingest_manager.granule_ingester import DatasetIngestionHistoryFile

HISTORY_ROOT_PATH = os.path.join(sys.prefix,
                                 ".sdap_ingest_manager",
                                 "tmp/history")
DATASET_ID = "zobi_la_mouche"


class DatasetIngestionHistoryFileTestCase(unittest.TestCase):

    # @unittest.skip("does not work without a solr server for test")
    def test_get_md5sum(self):
        ingestion_history = DatasetIngestionHistoryFile(HISTORY_ROOT_PATH, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("blue")

        self.assertEqual(result, "12weeukrhbwerqu7wier")

    # @unittest.skip("does not work without a solr server for test")
    def test_get_missing_md5sum(self):
        ingestion_history = DatasetIngestionHistoryFile(HISTORY_ROOT_PATH, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("green")

        self.assertEqual(result, None)


if __name__ == '__main__':
    unittest.main()
