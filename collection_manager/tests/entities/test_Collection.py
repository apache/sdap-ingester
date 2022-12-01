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

import json
import os
import unittest
from datetime import datetime, timezone

from collection_manager.entities import Collection
from collection_manager.entities.exceptions import MissingValueCollectionError


class TestCollection(unittest.TestCase):

    def test_directory_with_directory(self):
        directory = os.path.join(os.path.dirname(__file__), "../resources/data")
        collection = Collection(dataset_id="test_dataset",
                                path=directory,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertEqual(directory, collection.directory())

    def test_directory_with_pattern(self):
        pattern = os.path.join(os.path.dirname(__file__), "../resources/data/*.nc")
        collection = Collection(dataset_id="test_dataset",
                                path=pattern,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertEqual(os.path.dirname(pattern), collection.directory())

    def test_owns_file_raises_exception_with_directory(self):
        directory = os.path.join(os.path.dirname(__file__), "../resources/data")
        collection = Collection(dataset_id="test_dataset",
                                path=directory,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertRaises(IsADirectoryError, collection.owns_file, directory)

    def test_owns_file_matches(self):
        directory = os.path.join(os.path.dirname(__file__), "../resources/data")
        collection = Collection(dataset_id="test_dataset",
                                path=directory,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        file_path = os.path.join(directory, "test_file.nc")
        self.assertTrue(collection.owns_file(file_path))

    def test_owns_file_does_not_match(self):
        directory = os.path.join(os.path.dirname(__file__), "../resources/data")
        collection = Collection(dataset_id="test_dataset",
                                path=directory,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        self.assertFalse(collection.owns_file("test_file.nc"))

    def test_owns_file_matches_pattern(self):
        directory = os.path.join(os.path.dirname(__file__), "../resources/data")
        pattern = os.path.join(directory, "test_*.nc")
        collection = Collection(dataset_id="test_dataset",
                                path=pattern,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        file_path = os.path.join(directory, "test_file.nc")
        self.assertTrue(collection.owns_file(file_path))

    def test_owns_file_does_not_match_pattern(self):
        directory = os.path.join(os.path.dirname(__file__), "../resources/data")
        pattern = os.path.join(directory, "test_*.nc")
        collection = Collection(dataset_id="test_dataset",
                                path=pattern,
                                projection="Grid",
                                slices={},
                                dimension_names={},
                                historical_priority=1,
                                forward_processing_priority=2,
                                date_from=None,
                                date_to=None)
        file_path = os.path.join(directory, "nonmatch.nc")
        self.assertFalse(collection.owns_file(file_path))

    def test_from_dict(self):
        collection_dict = {
            'id': 'test_id',
            'path': '/some/path',
            'projection': 'Grid',
            'dimensionNames': {
                'latitude': 'lat',
                'longitude': 'lon',
                'variable': 'test_var'
            },
            'slices': {'lat': 30, 'lon': 30, 'time': 1},
            'priority': 1,
            'forward-processing-priority': 2,
            'from': '2020-01-01T00:00:00+00:00',
            'to': '2020-02-01T00:00:00+00:00'
        }

        expected_collection = Collection(dataset_id='test_id',
                                         projection="Grid",
                                         slices=frozenset([('lat', 30), ('lon', 30), ('time', 1)]),
                                         dimension_names=frozenset([
                                             ('latitude', 'lat'),
                                             ('longitude', 'lon'),
                                             ('variable', json.dumps('test_var'))
                                         ]),
                                         path='/some/path',
                                         historical_priority=1,
                                         forward_processing_priority=2,
                                         date_from=datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                                         date_to=datetime(2020, 2, 1, 0, 0, 0, tzinfo=timezone.utc))

        self.assertEqual(expected_collection, Collection.from_dict(collection_dict))

    def test_from_dict_dimension_list(self):
        collection_dict = {
            'id': 'test_id',
            'path': '/some/path',
            'projection': 'Grid',
            'dimensionNames': {
                'latitude': 'lat',
                'longitude': 'lon',
                'variables': ['test_var_1', 'test_var_2', 'test_var_3'],
            },
            'slices': {'lat': 30, 'lon': 30, 'time': 1},
            'priority': 1,
            'forward-processing-priority': 2,
            'from': '2020-01-01T00:00:00+00:00',
            'to': '2020-02-01T00:00:00+00:00'
        }

        expected_collection = Collection(dataset_id='test_id',
                                         projection="Grid",
                                         slices=frozenset([('lat', 30), ('lon', 30), ('time', 1)]),
                                         dimension_names=frozenset([
                                             ('latitude', 'lat'),
                                             ('longitude', 'lon'),
                                             ('variable', json.dumps(['test_var_1', 'test_var_2', 'test_var_3']))
                                         ]),
                                         path='/some/path',
                                         historical_priority=1,
                                         forward_processing_priority=2,
                                         date_from=datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                                         date_to=datetime(2020, 2, 1, 0, 0, 0, tzinfo=timezone.utc))

        self.assertEqual(expected_collection, Collection.from_dict(collection_dict))

    def test_from_dict_missing_optional_values(self):
        collection_dict = {
            'id': 'test_id',
            'projection': 'Grid',
            'dimensionNames': {
                'latitude': 'lat',
                'longitude': 'lon',
                'variable': 'test_var'
            },
            'slices': {'lat': 30, 'lon': 30, 'time': 1},
            'path': '/some/path',
            'priority': 3
        }

        expected_collection = Collection(dataset_id='test_id',
                                         projection="Grid",
                                         slices=frozenset([('lat', 30), ('lon', 30), ('time', 1)]),
                                         dimension_names=frozenset([
                                             ('latitude', 'lat'),
                                             ('longitude', 'lon'),
                                             ('variable', json.dumps('test_var'))
                                         ]),
                                         path='/some/path',
                                         historical_priority=3,
                                         forward_processing_priority=None,
                                         date_from=None,
                                         date_to=None)

        self.assertEqual(expected_collection, Collection.from_dict(collection_dict))

    def test_from_dict_missing_required_values(self):
        collection_dict = {
            'id': 'test_id',
            'variable': 'test_var',
            'path': '/some/path',
        }

        self.assertRaises(MissingValueCollectionError, Collection.from_dict, collection_dict)
