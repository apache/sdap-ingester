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

from granule_ingester.processors.reading_processors import EccoReadingProcessor


class TestEccoReadingProcessor(unittest.TestCase):

    def test_generate_tile(self):
        reading_processor = EccoReadingProcessor(variable=['OBP'],
                                                 latitude='YC',
                                                 longitude='XC',
                                                 time='time',
                                                 tile='tile')

        granule_path = path.join(path.dirname(__file__), '../granules/OBP_native_grid.nc')
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dimensions_to_slices = {
            'time': slice(0, 1),
            'tile': slice(10, 11),
            'j': slice(0, 15),
            'i': slice(0, 7)
        }
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(output_tile.summary.granule, granule_path)
            self.assertEqual(output_tile.tile.ecco_tile.tile, 10)
            self.assertEqual(output_tile.tile.ecco_tile.time, 695563200)
            self.assertEqual(output_tile.tile.ecco_tile.variable_data.shape, [15, 7])
            self.assertEqual(output_tile.tile.ecco_tile.latitude.shape, [15, 7])
            self.assertEqual(output_tile.tile.ecco_tile.longitude.shape, [15, 7])

    def test_generate_tile_with_dims_out_of_order(self):
        reading_processor = EccoReadingProcessor(variable=['OBP'],
                                                 latitude='YC',
                                                 longitude='XC',
                                                 time='time',
                                                 tile='tile')
        granule_path = path.join(path.dirname(__file__), '../granules/OBP_native_grid.nc')
        input_tile = nexusproto.NexusTile()

        dimensions_to_slices = {
            'j': slice(0, 15),
            'tile': slice(10, 11),
            'i': slice(0, 7),
            'time': slice(0, 1)
        }
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(output_tile.tile.ecco_tile.tile, 10)
            self.assertEqual(output_tile.tile.ecco_tile.time, 695563200)
            self.assertEqual(output_tile.tile.ecco_tile.variable_data.shape, [15, 7])
            self.assertEqual(output_tile.tile.ecco_tile.latitude.shape, [15, 7])
            self.assertEqual(output_tile.tile.ecco_tile.longitude.shape, [15, 7])
