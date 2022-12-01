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

import xarray as xr
import numpy as np
from os import path

from granule_ingester.processors.reading_processors import GridMultiVariableReadingProcessor, GridReadingProcessor
from nexusproto import DataTile_pb2 as nexusproto

from nexusproto.serialization import from_shaped_array, to_shaped_array

from granule_ingester.processors import ForceAscendingLatitude


class TestForceAscendingLatitude(unittest.TestCase):
    def test_01_grid_multi_band_data(self):
        reading_processor = GridMultiVariableReadingProcessor(['B03', 'B04'], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30)
        }

        with xr.open_dataset(granule_path) as ds:
            tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            flipped_tile = ForceAscendingLatitude().process(tile)
            the_flipped_tile_type = flipped_tile.tile.WhichOneof("tile_type")
            self.assertEqual(the_flipped_tile_type, 'grid_multi_variable_tile', f'wrong tile type')
            the_flipped_tile_data = getattr(flipped_tile.tile, the_flipped_tile_type)
            self.assertEqual([1, 30, 30, 2], the_flipped_tile_data.variable_data.shape)
            flipped_latitudes = from_shaped_array(the_flipped_tile_data.latitude)
            original_lat_data = ds['lat'].values
            np.testing.assert_almost_equal(flipped_latitudes[0], original_lat_data[29], decimal=5, err_msg='wrong first vs last latitude', verbose=True)
            np.testing.assert_almost_equal(flipped_latitudes[1], original_lat_data[28], decimal=5, err_msg='wrong latitude', verbose=True)
            flipped_data = from_shaped_array(the_flipped_tile_data.variable_data)
            original_b04_data = ds['B04'].values
            np.testing.assert_almost_equal(original_b04_data[0][0][0], flipped_data[0][29][0][1], decimal=4, err_msg='wrong first vs last data', verbose=True)
        return

    def test_02_grid_single_band_data(self):
        reading_processor = GridReadingProcessor(['B03'], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30)
        }

        with xr.open_dataset(granule_path) as ds:
            tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            flipped_tile = ForceAscendingLatitude().process(tile)
            the_flipped_tile_type = flipped_tile.tile.WhichOneof("tile_type")
            self.assertEqual(the_flipped_tile_type, 'grid_tile', f'wrong tile type')
            the_flipped_tile_data = getattr(flipped_tile.tile, the_flipped_tile_type)
            self.assertEqual([30, 30], the_flipped_tile_data.variable_data.shape)
            flipped_latitudes = from_shaped_array(the_flipped_tile_data.latitude)
            original_lat_data = ds['lat'].values
            np.testing.assert_almost_equal(flipped_latitudes[0], original_lat_data[29], decimal=5, err_msg='wrong first vs last latitude', verbose=True)
            np.testing.assert_almost_equal(flipped_latitudes[1], original_lat_data[28], decimal=5, err_msg='wrong latitude', verbose=True)
            flipped_data = from_shaped_array(the_flipped_tile_data.variable_data)
            original_b04_data = ds['B03'].values
            np.testing.assert_almost_equal(original_b04_data[0][0][0], flipped_data[29][0], decimal=4, err_msg='wrong first vs last data', verbose=True)
        return
