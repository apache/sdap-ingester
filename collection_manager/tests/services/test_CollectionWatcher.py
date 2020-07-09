import asyncio
import os
import tempfile
import unittest
from datetime import datetime
from unittest.mock import Mock

from collection_manager.entities import Collection
from collection_manager.entities.exceptions import CollectionConfigParsingError, CollectionConfigFileNotFoundError, \
    RelativePathCollectionError, ConflictingPathCollectionError
from collection_manager.services import CollectionWatcher


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
        flattened_collections = collection_watcher.collections()
        self.assertEqual(len(flattened_collections), 4)

    def test_load_collections_loads_all_collections(self):
        collections_path = os.path.join(os.path.dirname(__file__), '../resources/collections.yml')
        collection_watcher = CollectionWatcher(collections_path, Mock(), Mock())
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
        self.assertSetEqual(updated_collections, collection_watcher.collections())

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
                                variable="test_variable",
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
                                variable="test_variable",
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
                                variable="test_variable",
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertRaises(ConflictingPathCollectionError, collection_watcher._validate_collection, collection)

    def test_collection_callback_is_called(self):
        collections_config = tempfile.NamedTemporaryFile("w+b", buffering=0, delete=False)
        granule_dir = tempfile.TemporaryDirectory()
        collections_str = f"""collections:
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  variable: lwe_thickness
  priority: 1
  forward-processing-priority: 5"""
        collections_config.write(collections_str.encode("utf-8"))

        collection_callback = Mock()
        collection_watcher = CollectionWatcher(collections_path=collections_config.name,
                                               collection_updated_callback=collection_callback,
                                               granule_updated_callback=Mock(),
                                               collections_refresh_interval=0.1)

        loop = asyncio.new_event_loop()
        collection_watcher.start_watching(loop)

        collections_str = f"""
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  variable: lwe_thickness
  priority: 10
  forward-processing-priority: 5
        """
        collections_config.write(collections_str.encode("utf-8"))

        loop.run_until_complete(self.assert_called_within_timeout(collection_callback, call_count=2))

        loop.close()
        collections_config.close()
        granule_dir.cleanup()
        os.remove(collections_config.name)

    def test_granule_callback_is_called_on_new_file(self):
        with tempfile.NamedTemporaryFile("w+b", buffering=0) as collections_config:
            granule_dir = tempfile.TemporaryDirectory()
            collections_str = f"""
collections:
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  variable: lwe_thickness
  priority: 1
  forward-processing-priority: 5
            """
            collections_config.write(collections_str.encode("utf-8"))

            granule_callback = Mock()
            collection_watcher = CollectionWatcher(collections_config.name, Mock(), granule_callback)

            loop = asyncio.new_event_loop()
            collection_watcher.start_watching(loop)

            new_granule = open(os.path.join(granule_dir.name, 'test.nc'), "w+")

            loop.run_until_complete(self.assert_called_within_timeout(granule_callback))

            loop.close()
            new_granule.close()
            granule_dir.cleanup()

    def test_granule_callback_is_called_on_modified_file(self):
        with tempfile.NamedTemporaryFile("w+b", buffering=0) as collections_config:
            granule_dir = tempfile.TemporaryDirectory()
            collections_str = f"""
collections:
- id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
  path: {granule_dir.name}
  variable: lwe_thickness
  priority: 1
  forward-processing-priority: 5
            """
            collections_config.write(collections_str.encode("utf-8"))
            new_granule = open(os.path.join(granule_dir.name, 'test.nc'), "w+")

            granule_callback = Mock()
            collection_watcher = CollectionWatcher(collections_config.name, Mock(), granule_callback)

            loop = asyncio.new_event_loop()
            collection_watcher.start_watching(loop)

            new_granule.write("hello world")
            new_granule.close()

            loop.run_until_complete(self.assert_called_within_timeout(granule_callback))
            loop.close()
            granule_dir.cleanup()

    def test_run_periodically(self):
        callback = Mock()
        loop = asyncio.new_event_loop()
        CollectionWatcher._run_periodically(loop, 0.1, callback)
        loop.run_until_complete(self.assert_called_within_timeout(callback, timeout_sec=0.3, call_count=2))
        loop.close()

    @staticmethod
    async def assert_called_within_timeout(mock_func, timeout_sec=1.0, call_count=1):
        start = datetime.now()

        while (datetime.now() - start).total_seconds() < timeout_sec:
            await asyncio.sleep(0.01)
            if mock_func.call_count >= call_count:
                return
        raise AssertionError(f"{mock_func} did not reach {call_count} calls called within {timeout_sec} sec")

