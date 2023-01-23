# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from collection_manager.services.history_manager import SolrIngestionHistory

SOLR_URL = "http://localhost:8984/solr"
DATASET_ID = "zobi_la_mouche"


# TODO: mock solr and fix these tests
class TestSolrIngestionHistory(unittest.TestCase):
    @unittest.skip("does not work without a solr server for history_manager")
    def test_get(self):
        ingestion_history = SolrIngestionHistory(SOLR_URL, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get("blue")

        self.assertEqual(result.docs[0]['dataset_s'], "zobi_la_mouche")
        self.assertEqual(result.docs[0]['granule_s'], "blue")
        self.assertEqual(result.docs[0]['granule_md5sum_s'], "12weeukrhbwerqu7wier")

    @unittest.skip("does not work without a solr server for history_manager")
    def test_get_md5sum(self):
        ingestion_history = SolrIngestionHistory(SOLR_URL, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("blue")

        self.assertEqual(result, "12weeukrhbwerqu7wier")

    @unittest.skip("does not work without a solr server for history_manager")
    def test_get_missing_md5sum(self):
        ingestion_history = SolrIngestionHistory(SOLR_URL, DATASET_ID)

        ingestion_history.push("blue", "12weeukrhbwerqu7wier")

        result = ingestion_history.get_md5sum("green")

        self.assertEqual(result, None)


if __name__ == '__main__':
    unittest.main()
