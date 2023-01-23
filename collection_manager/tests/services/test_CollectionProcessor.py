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

import tempfile
import yaml
import unittest
from unittest import mock

from collection_manager.entities import Collection
from collection_manager.services import CollectionProcessor
from collection_manager.services.history_manager import FileIngestionHistoryBuilder
from collection_manager.services.history_manager import GranuleStatus
from common.async_test_utils import AsyncMock, async_test


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
        expected = {
            'granule': {
                'resource': '/granules/test_granule.nc'
            },
            'processors': [
                {
                    'latitude': 'lat',
                    'longitude': 'lon',
                    'name': 'Grid',
                    'variable': 'test_var'
                },
                {'name': 'emptyTileFilter'},
                {'dataset_name': 'test_dataset', 'name': 'tileSummary'},
                {'name': 'generateTileId'}
            ],
            'slicer': {
                'dimension_step_sizes': {
                    'lat': 30,
                    'lon': 30,
                    'time': 1
                },
                'name': 'sliceFileByStepSize'
            }
        }
        collection = Collection(dataset_id="test_dataset",
                                path="/granules/test*.nc",
                                projection="Grid",
                                slices=frozenset([('lat', 30), ('lon', 30), ('time', 1)]),
                                dimension_names=frozenset([
                                    ('latitude', 'lat'),
                                    ('longitude', 'lon'),
                                    ('variable', 'test_var')
                                ]),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        filled = CollectionProcessor._generate_ingestion_message("/granules/test_granule.nc", collection)
        generated_yaml = yaml.load(filled, Loader=yaml.FullLoader)

        self.assertEqual(expected, generated_yaml)

    @async_test
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', new_callable=AsyncMock)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', new_callable=AsyncMock)
    async def test_process_granule_with_historical_granule(self, mock_publisher, mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.DESIRED_HISTORICAL
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        await collection_processor.process_granule("test.nc", collection)

        mock_publisher.publish_message.assert_called_with(body=mock.ANY, priority=1)
        mock_history.push.assert_called()

    @async_test
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', new_callable=AsyncMock)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder',  autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', new_callable=AsyncMock)
    async def test_process_granule_with_forward_processing_granule(self,
                                                                   mock_publisher,
                                                                   mock_history_builder,
                                                                   mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.DESIRED_FORWARD_PROCESSING
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        await collection_processor.process_granule("test.h5", collection)

        mock_publisher.publish_message.assert_called_with(body=mock.ANY, priority=2)
        mock_history.push.assert_called()

    @async_test
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', new_callable=AsyncMock)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', new_callable=AsyncMock)
    async def test_process_granule_with_forward_processing_granule_and_no_priority(self, mock_publisher,
                                                                                   mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.DESIRED_FORWARD_PROCESSING
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                date_from=None,
                                date_to=None)

        await collection_processor.process_granule("test.h5", collection)

        mock_publisher.publish_message.assert_called_with(body=mock.ANY, priority=1)
        mock_history.push.assert_called()

    @async_test
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', new_callable=AsyncMock)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', new_callable=AsyncMock)
    async def test_process_granule_with_undesired_granule(self, mock_publisher, mock_history_builder, mock_history):
        mock_history.get_granule_status.return_value = GranuleStatus.UNDESIRED
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        await collection_processor.process_granule("test.nc", collection)

        mock_publisher.publish_message.assert_not_called()
        mock_history.push.assert_not_called()

    @async_test
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistory', autospec=True)
    @mock.patch('collection_manager.services.history_manager.FileIngestionHistoryBuilder', autospec=True)
    @mock.patch('collection_manager.services.MessagePublisher', new_callable=AsyncMock)
    async def test_process_granule_with_unsupported_file_type(self, mock_publisher, mock_history_builder, mock_history):
        mock_history_builder.build.return_value = mock_history

        collection_processor = CollectionProcessor(mock_publisher, mock_history_builder)
        collection = Collection(dataset_id="test_dataset",
                                path="test_path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)

        await collection_processor.process_granule("test.foo", collection)

        mock_publisher.publish_message.assert_not_called()
        mock_history.push.assert_not_called()
