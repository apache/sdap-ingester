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

import numpy as np
import xarray as xr
from granule_ingester.processors import ForceAscendingLatitude
from granule_ingester.processors.EmptyTileFilter import EmptyTileFilter
from granule_ingester.processors.Subtract180FromLongitude import Subtract180FromLongitude
from granule_ingester.processors.TileSummarizingProcessor import TileSummarizingProcessor
from granule_ingester.processors.kelvintocelsius import KelvinToCelsius
from granule_ingester.processors.reading_processors import GridMultiBandReadingProcessor
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array


class TestReadHLSData(unittest.TestCase):

    def test_01(self):
        reading_processor = GridMultiBandReadingProcessor([f'B{k:02d}' for k in range(1, 12)], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            # 'lat': slice(0, 200),
            # 'lon': slice(0, 200),
        }

        with xr.open_dataset(granule_path) as ds:
            generated_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
        tile_type = generated_tile.tile.WhichOneof("tile_type")
        tile_data = getattr(generated_tile.tile, tile_type)
        # latitudes = from_shaped_array(tile_data.latitude)
        # longitudes = from_shaped_array(tile_data.longitude)
        # variable_data = from_shaped_array(tile_data.variable_data)
        # print(generated_tile.tile.grid_tile.variable_data)
        self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
        self.assertEqual(1577836800, generated_tile.tile.grid_tile.time)
        self.assertEqual([11, 30, 30], generated_tile.tile.grid_tile.variable_data.shape)
        self.assertEqual([30], generated_tile.tile.grid_tile.latitude.shape)
        self.assertEqual([30], generated_tile.tile.grid_tile.longitude.shape)
        return

    def test_02_preprocessed_data(self):
        reading_processor = GridMultiBandReadingProcessor([f'b{k}' for k in range(2, 8)], 'lat', 'long', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/s1_output_latlon_HLS_S30_T18TYN_2019363.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(500, 550),
            'long': slice(500, 550),
        }

        with xr.open_dataset(granule_path) as ds:
            generated_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
        empty_filter = EmptyTileFilter().process(generated_tile)
        self.assertNotEqual(empty_filter, None, f'empty_filter is None')
        subtract_180 = Subtract180FromLongitude().process(empty_filter)
        self.assertNotEqual(subtract_180, None, f'subtract_180 is None')
        variable_data = from_shaped_array(subtract_180.tile.grid_tile.variable_data)
        with open('before.txt', 'w') as ff:
            ff.write(str(list(variable_data)))
        force_asc = ForceAscendingLatitude().process(empty_filter)
        self.assertNotEqual(force_asc, None, f'force_asc is None')
        variable_data = from_shaped_array(force_asc.tile.grid_tile.variable_data)
        with open('after.txt', 'w') as ff:
            ff.write(str(list(variable_data)))
        kelvin = KelvinToCelsius().process(force_asc)
        self.assertNotEqual(kelvin, None, f'kelvin is None')
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            kelvin.summary.data_var_name.extend([f'b{k}' for k in range(2, 8)])
            summary = TileSummarizingProcessor('test').process(kelvin, ds)
            self.assertNotEqual(summary, None, f'summary is None')

        tile_type = generated_tile.tile.WhichOneof("tile_type")
        tile_data = getattr(generated_tile.tile, tile_type)
        latitudes = from_shaped_array(tile_data.latitude)
        # print(latitudes)
        # longitudes = from_shaped_array(tile_data.longitude)
        variable_data = from_shaped_array(tile_data.variable_data)
        # print(variable_data)
        self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
        self.assertEqual(1577577600, generated_tile.tile.grid_tile.time)
        self.assertEqual([6, 50, 50], generated_tile.tile.grid_tile.variable_data.shape)
        self.assertEqual([50], generated_tile.tile.grid_tile.latitude.shape)
        self.assertEqual([50], generated_tile.tile.grid_tile.longitude.shape)

        return

    def test_03(self):
        reading_processor = GridMultiBandReadingProcessor(['B03'], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30)
        }

        with xr.open_dataset(granule_path) as ds:
            generated_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
        tile_type = generated_tile.tile.WhichOneof("tile_type")
        tile_data = getattr(generated_tile.tile, tile_type)
        latitudes = from_shaped_array(tile_data.latitude)
        longitudes = from_shaped_array(tile_data.longitude)
        variable_data = from_shaped_array(tile_data.variable_data)

        self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
        self.assertEqual(1577836800, generated_tile.tile.grid_tile.time)
        self.assertEqual([30, 30], generated_tile.tile.grid_tile.variable_data.shape)
        self.assertEqual([30], generated_tile.tile.grid_tile.latitude.shape)
        self.assertEqual([30], generated_tile.tile.grid_tile.longitude.shape)

        # print(latitudes)
        # print(longitudes)
        # print(variable_data)
        return

    def test_04(self):
        self.assertRaises(RuntimeError, GridMultiBandReadingProcessor, [], 'lat', 'lon', time='time')
        return


if __name__ == '__main__':
    unittest.main()
