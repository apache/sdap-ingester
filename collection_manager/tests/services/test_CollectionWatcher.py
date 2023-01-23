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
import tempfile
import unittest
from datetime import datetime
from unittest.mock import Mock

from collection_manager.entities import Collection
from collection_manager.entities.exceptions import CollectionConfigParsingError, CollectionConfigFileNotFoundError, \
    RelativePathCollectionError, ConflictingPathCollectionError
from collection_manager.services import CollectionWatcher
from common.async_test_utils.AsyncTestUtils import AsyncAssert, AsyncMock, async_test


class TestCollectionWatcher(unittest.TestCase):

    def test_collections_returns_all_collections(self):
        collection_watcher = CollectionWatcher('/foo', Mock(), Mock())
        collection_watcher._collections_by_dir = {
            "/foo": {
                Collection("id1", "var1", "path1", 1, 2, datetime.now(), datetime.now()),
                Collection("id2", "var2", "path2", 3, 4, datetime.now(), datetime.now()),
            },
            "/bar": {
                Collection("id3", "var3", "path3", 5, 6, datetime.now(), datetime.now()),
                Collection("id4", "var4", "path4", 7, 8, datetime.now(), datetime.now()),
            }
        }
        flattened_collections = collection_watcher._collections()
        self.assertEqual(len(flattened_collections), 4)

    def test_load_collections_loads_all_collections(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, AsyncMock(), AsyncMock())
        collection_watcher._load_collections()

        self.assertEqual(len(collection_watcher._collections_by_dir), 2)
        self.assertEqual(len(collection_watcher._collections_by_dir['/opt/data/grace']), 2)
        self.assertEqual(len(collection_watcher._collections_by_dir['/opt/data/avhrr']), 1)

    def test_load_collections_with_bad_yaml_syntax(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections_bad_syntax.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        self.assertRaises(CollectionConfigParsingError, collection_watcher._load_collections)

    def test_load_collections_with_bad_schema(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections_bad_schema.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        self.assertRaises(CollectionConfigParsingError, collection_watcher._load_collections)

    def test_load_collections_with_file_not_found(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/does_not_exist.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        self.assertRaises(CollectionConfigFileNotFoundError, collection_watcher._load_collections)

    def test_get_updated_collections_returns_all_collections(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        updated_collections = collection_watcher._get_updated_collections()
        self.assertSetEqual(updated_collections, collection_watcher._collections())

    def test_get_updated_collections_returns_no_collections(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())
        collection_watcher._load_collections()
        updated_collections = collection_watcher._get_updated_collections()

        self.assertEqual(len(updated_collections), 0)

    def test_get_updated_collections_returns_some_collections(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())
        collection_watcher._load_collections()

        new_collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections_alternate.yml')
        collection_watcher._collections_path = new_collections_path
        updated_collections = collection_watcher._get_updated_collections()

        self.assertEqual(len(updated_collections), 1)

    def test_validate_collection(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        collection = Collection(dataset_id="test_dataset",
                                path="/absolute/path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        collection_watcher._validate_collection(collection)

    def test_validate_collection_with_relative_path(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        collection = Collection(dataset_id="test_dataset",
                                path="relative/path",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertRaises(RelativePathCollectionError, collection_watcher._validate_collection, collection)

    def test_validate_collection_with_conflicting_path(self):
        collections_path = os.path.join(os.path.dirname(__file__), '/resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())

        collection = Collection(dataset_id="test_dataset",
                                path="/resources/*.nc",
                                projection="Grid",
                                slices=frozenset(),
                                dimension_names=frozenset(),
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertRaises(ConflictingPathCollectionError, collection_watcher._validate_collection, collection)

    @async_test
    async def test_collection_callback_is_called(self):
        collections_config = tempfile.NamedTemporaryFile("w+b", buffering=0, delete=False)
        granule_dir = tempfile.TemporaryDirectory()
        collections_str = f"""collections:
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  priority: 1
  forward-processing-priority: 5
  projection: Grid
  dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
  slices:
      time: 1
      lat: 30
      lon: 30
  """
        collections_config.write(collections_str.encode("utf-8"))

        collection_callback = AsyncMock()
        collection_watcher = CollectionWatcher(collections_path=collections_config.name,
                                               collection_updated_callback=collection_callback,
                                               granule_updated_callback=AsyncMock(),
                                               collections_refresh_interval=0.1)

        await collection_watcher.start_watching()

        collections_str = f"""
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  priority: 10
  forward-processing-priority: 5
  projection: Grid
  dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
  slices:
      time: 1
      lat: 30
      lon: 30
        """
        collections_config.write(collections_str.encode("utf-8"))

        await AsyncAssert.assert_called_within_timeout(collection_callback, call_count=2)

        collections_config.close()
        granule_dir.cleanup()
        os.remove(collections_config.name)

    @async_test
    async def test_granule_callback_is_called_on_new_file(self):
        with tempfile.NamedTemporaryFile("w+b", buffering=0) as collections_config:
            granule_dir = tempfile.TemporaryDirectory()
            collections_str = f"""
collections:
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  priority: 1
  forward-processing-priority: 5
  projection: Grid
  dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
  slices:
      time: 1
      lat: 30
      lon: 30
            """
            collections_config.write(collections_str.encode("utf-8"))

            granule_callback = AsyncMock()
            collection_watcher = CollectionWatcher(collections_config.name, AsyncMock(), granule_callback)

            await collection_watcher.start_watching()
            new_granule = open(os.path.join(granule_dir.name, 'test.nc'), "w+")
            await AsyncAssert.assert_called_within_timeout(granule_callback)

            new_granule.close()
            granule_dir.cleanup()

    @async_test
    async def test_granule_callback_is_called_on_modified_file(self):
        with tempfile.NamedTemporaryFile("w+b", buffering=0) as collections_config:
            granule_dir = tempfile.TemporaryDirectory()
            collections_str = f"""
collections:
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  priority: 1
  forward-processing-priority: 5
  projection: Grid
  dimensionNames:
      latitude: lat
      longitude: lon
      time: time
      variable: lwe_thickness
  slices:
      time: 1
      lat: 30
      lon: 30
            """
            collections_config.write(collections_str.encode("utf-8"))
            new_granule = open(os.path.join(granule_dir.name, 'test.nc'), "w+")

            granule_callback = AsyncMock()
            collection_watcher = CollectionWatcher(collections_config.name, AsyncMock(), granule_callback)

            await collection_watcher.start_watching()

            new_granule.write("hello world")
            new_granule.close()

            await AsyncAssert.assert_called_within_timeout(granule_callback)
            granule_dir.cleanup()

    @async_test
    async def test_run_periodically(self):
        callback = AsyncMock()
        await CollectionWatcher._run_periodically(None, 0.1, callback)
        await AsyncAssert.assert_called_within_timeout(callback, timeout_sec=0.3, call_count=2)
