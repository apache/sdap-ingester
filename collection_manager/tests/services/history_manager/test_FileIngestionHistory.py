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
            self.assertTrue(await ingestion_history.already_ingested(str(current_file_path)))

            del ingestion_history

    @async_test
    async def test_already_ingested_with_latest_modifcation_signature(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, os.path.getmtime)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            await ingestion_history.push(str(current_file_path))
            self.assertTrue(await ingestion_history.already_ingested(str(current_file_path)))

            del ingestion_history

    @async_test
    async def test_already_ingested_is_false(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            self.assertFalse(await ingestion_history.already_ingested(str(current_file_path)))


if __name__ == '__main__':
    unittest.main()
