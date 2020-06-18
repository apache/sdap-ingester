import os
import pathlib
import tempfile
import unittest

from collection_manager.services.history_manager import FileIngestionHistory, md5sum_from_filepath

DATASET_ID = "zobi_la_mouche"


class TestFileIngestionHistory(unittest.TestCase):

    def test_get_md5sum(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
            result = ingestion_history._get_signature("blue")
            self.assertEqual(result, "12weeukrhbwerqu7wier")

    def test_get_missing_md5sum(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            ingestion_history._push_record("blue", "12weeukrhbwerqu7wier")
            result = ingestion_history._get_signature("green")
            self.assertIsNone(result)

    def test_already_ingested(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            ingestion_history.push(str(current_file_path))
            self.assertTrue(ingestion_history.already_ingested(str(current_file_path)))

            del ingestion_history

    def test_already_ingested_with_latest_modifcation_signature(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, os.path.getmtime)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            ingestion_history.push(str(current_file_path))
            self.assertTrue(ingestion_history.already_ingested(str(current_file_path)))

            del ingestion_history

    def test_already_ingested_is_false(self):
        with tempfile.TemporaryDirectory() as history_dir:
            ingestion_history = FileIngestionHistory(history_dir, DATASET_ID, md5sum_from_filepath)
            # history_manager with this file
            current_file_path = pathlib.Path(__file__)
            self.assertFalse(ingestion_history.already_ingested(str(current_file_path)))


if __name__ == '__main__':
    unittest.main()
