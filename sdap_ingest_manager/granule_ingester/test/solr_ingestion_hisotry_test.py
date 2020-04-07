import unittest
from sdap_ingest_manager.granule_ingester import SolrIngestionHistory

SOLR_URL = "http://localhost:8984/solr"


class SolrIngestionHistoryTestCase(unittest.TestCase):
    @unittest.skip("requires a solr server to work")
    def test_get(self):
        ingestion_history = SolrIngestionHistory(SOLR_URL)

        ingestion_history.push("zobi_la_mouche", "blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get("zobi_la_mouche", "blue")

        self.assertEqual(result.docs[0]['dataset_s'], "zobi_la_mouche")
        self.assertEqual(result.docs[0]['granule_s'], "blue")
        self.assertEqual(result.docs[0]['granule_md5sum_s'], "12weeukrhbwerqu7wier")

    @unittest.skip("requires a solr server to work")
    def test_get_md5sum(self):
        ingestion_history = SolrIngestionHistory(SOLR_URL)

        ingestion_history.push("zobi_la_mouche", "blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("zobi_la_mouche", "blue")

        self.assertEqual(result, "12weeukrhbwerqu7wier")


if __name__ == '__main__':
    unittest.main()
