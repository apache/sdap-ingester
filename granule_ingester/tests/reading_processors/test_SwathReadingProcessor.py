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
from os import path

import xarray as xr
from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.processors.reading_processors import SwathReadingProcessor


class TestReadAscatbData(unittest.TestCase):
    def test_read_not_empty_ascatb(self):
        reading_processor = SwathReadingProcessor(variable='wind_speed',
                                                  latitude='lat',
                                                  longitude='lon',
                                                  time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_ascatb.nc4')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'NUMROWS': slice(0, 1),
            'NUMCELLS': slice(0, 82)
        }
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual([1, 82], output_tile.tile.swath_tile.time.shape)
            self.assertEqual([1, 82], output_tile.tile.swath_tile.variable_data.shape)
            self.assertEqual([1, 82], output_tile.tile.swath_tile.latitude.shape)
            self.assertEqual([1, 82], output_tile.tile.swath_tile.longitude.shape)


class TestReadSmapData(unittest.TestCase):
    def test_read_not_empty_smap(self):
        reading_processor = SwathReadingProcessor(
            variable='smap_sss',
            latitude='lat',
            longitude='lon',
            time='row_time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_smap.h5')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'phony_dim_0': slice(0, 38),
            'phony_dim_1': slice(0, 1)
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule)
            self.assertEqual([1], output_tile.tile.swath_tile.time.shape)
            self.assertEqual([38, 1], output_tile.tile.swath_tile.variable_data.shape)
            self.assertEqual([38, 1], output_tile.tile.swath_tile.latitude.shape)
            self.assertEqual([38, 1], output_tile.tile.swath_tile.longitude.shape)
