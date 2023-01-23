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

import os
import pathlib
import tempfile
import unittest

from collection_manager.services.history_manager import (FileIngestionHistory,
                                                         md5sum_from_filepath)

from common.async_test_utils.AsyncTestUtils import async_test

DATASET_ID = "zobi_la_mouche"


class TestFileIngestionHistory(unittest.TestCase):

    @async_test
    async def test_get_md5sum(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            await ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
            result = await ingestion_history._get_signature("blue")
            self.assertEqual(result, "12weeukrhbwerqu7wier")

    @async_test
    async def test_get_missing_md5sum(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            await ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
            result = await ingestion_history._get_signature("green")
            self.assertIsNone(result)

    @async_test
    async def test_already_ingested(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            await ingestion_history.push(str(current_file_path))
            self.assertTrue(await ingestion_history._already_ingested(str(current_file_path)))

            del ingestion_history

    @async_test
    async def test_already_ingested_with_latest_modifcation_signature(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, os.path.getmtime)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            await ingestion_history.push(str(current_file_path))
            self.assertTrue(await ingestion_history._already_ingested(str(current_file_path)))

            del ingestion_history

    @async_test
    async def test_already_ingested_is_false(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            self.assertFalse(await ingestion_history._already_ingested(str(current_file_path)))


if __name__ == '__main__':
    unittest.main()
