import unittest
from sdap_ingest_manager.history_manager import DatasetIngestionHistorySolr

SOLR_URL = "http://localhost:8984/solr"
DATASET_ID = "zobi_la_mouche"


class DatasetIngestionHistorySolrTestCase(unittest.TestCase):
    @unittest.skip("does not work without a solr server for history_manager")
    def test_get(self):
        ingestion_history = DatasetIngestionHistorySolr(SOLR_URL, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get("blue")

        self.assertEqual(result.docs[0]['dataset_s'], "zobi_la_mouche")
        self.assertEqual(result.docs[0]['granule_s'], "blue")
        self.assertEqual(result.docs[0]['granule_md5sum_s'], "12weeukrhbwerqu7wier")

    @unittest.skip("does not work without a solr server for history_manager")
    def test_get_md5sum(self):
        ingestion_history = DatasetIngestionHistorySolr(SOLR_URL, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("blue")

        self.assertEqual(result, "12weeukrhbwerqu7wier")

    @unittest.skip("does not work without a solr server for history_manager")
    def test_get_missing_md5sum(self):
        ingestion_history = DatasetIngestionHistorySolr(SOLR_URL, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("green")

        self.assertEqual(result, None)


if __name__ == '__main__':
    unittest.main()
