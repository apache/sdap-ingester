import tempfile
import unittest
from unittest import mock

from collection_manager.entities import Collection
from collection_manager.services import CollectionProcessor
from collection_manager.services.history_manager import FileIngestionHistoryBuilder
from collection_manager.services.history_manager import GranuleStatus


class TestCollectionProcessor(unittest.TestCase):

    def test_file_supported_with_nc(self):
        self.assertTrue(CollectionProcessor._file_supported("test_dir/test_granule.nc"))

    def test_file_supported_with_h5(self):
        self.assertTrue(CollectionProcessor._file_supported("test_dir/test_granule.h5"))

    def test_file_supported_with_foo(self):
        self.assertFalse(CollectionProcessor._file_supported("test_dir/test_granule.foo"))

    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_get_history_manager_returns_same_object(self, mock_publisher):
        with tempfile.TemporaryDirectory() as history_dir:
            collection_processor = CollectionProcessor(mock_publisher, FileIngestionHistoryBuilder(history_dir))
            history_manager = collection_processor._get_history_manager('dataset_id')
            self.assertIs(collection_processor._get_history_manager('dataset_id'), history_manager)

    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_get_history_manager_returns_different_object(self, mock_publisher):
        with tempfile.TemporaryDirectory() as history_dir:
            collection_processor = CollectionProcessor(mock_publisher, FileIngestionHistoryBuilder(history_dir))
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
          resource: /granules/test_granule.nc
        processors:
          - name: GridReadingProcessor
            variable_to_read: test_variable
          - name: tileSummary
            dataset_name: test_dataset
            """
        collection = Collection(dataset_id="test_dataset",
                                path="/granules/test*.nc",
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        filled = CollectionProcessor._fill_template("/granules/test_granule.nc", collection, template)
        self.assertEqual(filled, expected)

    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', autospec=True)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_process_granule_with_historical_granule(self, mock_publisher, mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.DESIRED_HISTORICAL
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        collection_processor.process_granule("test.nc", collection)

        mock_publisher.publish_message.assert_called_with(body=mock.ANY, priority=1)
        mock_history.push.assert_called()

    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', autospec=True)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_process_granule_with_forward_processing_granule(self, mock_publisher, mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.DESIRED_FORWARD_PROCESSING
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        collection_processor.process_granule("test.h5", collection)

        mock_publisher.publish_message.assert_called_with(body=mock.ANY, priority=2)
        mock_history.push.assert_called()

    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', autospec=True)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_process_granule_with_forward_processing_granule_and_no_priority(self, mock_publisher,
                                                                             mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.DESIRED_FORWARD_PROCESSING
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                variable="test_variable",
                                historical_priority=1,
                                date_from=None,
                                date_to=None)

        collection_processor.process_granule("test.h5", collection)

        mock_publisher.publish_message.assert_called_with(body=mock.ANY, priority=1)
        mock_history.push.assert_called()

    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', autospec=True)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_process_granule_with_undesired_granule(self, mock_publisher, mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.UNDESIRED
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        collection_processor.process_granule("test.nc", collection)

        mock_publisher.publish_message.assert_not_called()
        mock_history.push.assert_not_called()

    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', autospec=True)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', autospec=True)
    def test_process_granule_with_unsupported_file_type(self, mock_publisher, mock_history_builder, mock_history):
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        collection_processor.process_granule("test.foo", collection)

        mock_publisher.publish_message.assert_not_called()
        mock_history.push.assert_not_called()
