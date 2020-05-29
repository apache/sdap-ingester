import tempfile
from unittest import mock
import unittest

from collection_manager.collection_manager.entities import Collection
from collection_manager.collection_manager.services import CollectionProcessor
from collection_manager.collection_manager.services.history_manager import FileIngestionHistoryBuilder, FileIngestionHistory


class TestCollectionProcessor(unittest.TestCase):

    def test_file_supported_with_nc(self):
        self.assertTrue(CollectionProcessor._file_supported("test_dir/test_granule.nc"))

    def test_file_supported_with_h5(self):
        self.assertTrue(CollectionProcessor._file_supported("test_dir/test_granule.h5"))

    def test_file_supported_with_foo(self):
        self.assertFalse(CollectionProcessor._file_supported("test_dir/test_granule.foo"))

    def test_get_history_manager_returns_same_object(self):
        with tempfile.TemporaryDirectory() as history_dir:
            collection_processor = CollectionProcessor(None, FileIngestionHistoryBuilder(history_dir))
            history_manager = collection_processor._get_history_manager('dataset_id')
            self.assertIs(collection_processor._get_history_manager('dataset_id'), history_manager)

    def test_get_history_manager_returns_different_object(self):
        with tempfile.TemporaryDirectory() as history_dir:
            collection_processor = CollectionProcessor(None, FileIngestionHistoryBuilder(history_dir))
            history_manager = collection_processor._get_history_manager('foo')
            self.assertIsNot(collection_processor._get_history_manager('bar'), history_manager)

    def test_fill_template(self):
        template = """
        granule:
          resource: {{granule}}
        processors:
          - name: GridReadingProcessor
            variable_to_read: {{variable}}
          - name: tileSummary
            dataset_name: {{dataset_id}}
            """

        expected = """
        granule:
          resource: test_path
        processors:
          - name: GridReadingProcessor
            variable_to_read: test_variable
          - name: tileSummary
            dataset_name: test_dataset
            """
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        filled = CollectionProcessor._fill_template(collection, template)
        self.assertEqual(filled, expected)

    @mock.patch.object(FileIngestionHistory, 'push')
    @mock.patch.object(FileIngestionHistory, 'get_granule_status')
    def test_process_granule(self):
        history_manager_builder = FileIngestionHistoryBuilder('/foo')
