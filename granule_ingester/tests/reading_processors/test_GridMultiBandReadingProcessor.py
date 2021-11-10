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
import unittest
from os import path

import numpy as np
import xarray as xr
from granule_ingester.processors import ForceAscendingLatitude
from granule_ingester.processors.EmptyTileFilter import EmptyTileFilter
from granule_ingester.processors.Subtract180FromLongitude import Subtract180FromLongitude
from granule_ingester.processors.TileSummarizingProcessor import TileSummarizingProcessor
from granule_ingester.processors.kelvintocelsius import KelvinToCelsius
from granule_ingester.processors.reading_processors import GridMultiVariableReadingProcessor
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array


class TestReadHLSData(unittest.TestCase):

    def test_01(self):
        reading_processor = GridMultiVariableReadingProcessor([f'B{k:02d}' for k in range(1, 12)], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30),
        }

        with xr.open_dataset(granule_path) as ds:
            generated_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
            tile_type = generated_tile.tile.WhichOneof("tile_type")
            self.assertEqual(tile_type, 'grid_multi_variable_tile', f'wrong tile type')
            tile_data = getattr(generated_tile.tile, tile_type)
            self.assertEqual(1577836800, tile_data.time)
            self.assertEqual([1, 30, 30, 11], tile_data.variable_data.shape)
            self.assertEqual([30], tile_data.latitude.shape)
            self.assertEqual([30], tile_data.longitude.shape)
            variable_data = from_shaped_array(tile_data.variable_data)
            original_b03_data = ds['B03'].values
            self.assertEqual(original_b03_data[0][1][0], variable_data[0][1][0][2])
        return

    def test_02_preprocessed_data(self):
        reading_processor = GridMultiVariableReadingProcessor([f'b{k}' for k in range(2, 8)], 'lat', 'long', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/s1_output_latlon_HLS_S30_T18TYN_2019363.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 550),
            'long': slice(0, 550),
        }

        with xr.open_dataset(granule_path) as ds:
            generated_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
        empty_filter = EmptyTileFilter().process(generated_tile)
        self.assertNotEqual(empty_filter, None, f'empty_filter is None')
        subtract_180 = Subtract180FromLongitude().process(empty_filter)
        self.assertNotEqual(subtract_180, None, f'subtract_180 is None')
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
            tile_type = generated_tile.tile.WhichOneof("tile_type")
            self.assertEqual(tile_type, 'grid_multi_variable_tile', f'wrong tile type')
            tile_data = getattr(generated_tile.tile, tile_type)
            self.assertEqual(1577577600, tile_data.time)
            self.assertEqual([1, 550, 550, 6], tile_data.variable_data.shape)
            self.assertEqual([550], tile_data.latitude.shape)
            self.assertEqual([550], tile_data.longitude.shape)
            variable_data = from_shaped_array(tile_data.variable_data)
            original_b2_data = ds['b2'].values
            self.assertEqual(original_b2_data[0][500][104], variable_data[0][500][104][0])
        return

    def test_02_a_preprocessed_data_chain_processors(self):
        reading_processor = GridMultiVariableReadingProcessor([f'b{k}' for k in range(2, 8)], 'lat', 'long', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/s1_output_latlon_HLS_S30_T18TYN_2019363.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 550),
            'long': slice(0, 550),
        }

        with xr.open_dataset(granule_path) as ds:
            generated_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
        empty_filter = EmptyTileFilter().process(generated_tile)
        self.assertNotEqual(empty_filter, None, f'empty_filter is None')
        subtract_180 = Subtract180FromLongitude().process(empty_filter)
        self.assertNotEqual(subtract_180, None, f'subtract_180 is None')
        force_asc = ForceAscendingLatitude().process(empty_filter)
        self.assertNotEqual(force_asc, None, f'force_asc is None')
        kelvin = KelvinToCelsius().process(force_asc)
        self.assertNotEqual(kelvin, None, f'kelvin is None')
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            kelvin.summary.data_var_name = json.dumps([f'b{k}' for k in range(2, 8)])
            summary = TileSummarizingProcessor('test').process(kelvin, ds)
            self.assertNotEqual(summary, None, f'summary is None')
            self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
            tile_type = generated_tile.tile.WhichOneof("tile_type")
            self.assertEqual(tile_type, 'grid_multi_variable_tile', f'wrong tile type')
            tile_data = getattr(generated_tile.tile, tile_type)
            self.assertEqual(1577577600, tile_data.time)
            self.assertEqual([1, 550, 550, 6], tile_data.variable_data.shape)
            self.assertEqual([550], tile_data.latitude.shape)
            self.assertEqual([550], tile_data.longitude.shape)
        return

    def test_03(self):
        reading_processor = GridMultiVariableReadingProcessor(['B03'], 'lat', 'lon', time='time')
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
        self.assertEqual(tile_type, 'grid_multi_variable_tile', f'wrong tile type')
        tile_data = getattr(generated_tile.tile, tile_type)

        self.assertEqual(granule_path, generated_tile.summary.granule, granule_path)
        self.assertEqual(1577836800, tile_data.time)
        self.assertEqual([1, 30, 30, 1], tile_data.variable_data.shape)
        self.assertEqual([30], tile_data.latitude.shape)
        self.assertEqual([30], tile_data.longitude.shape)
        variable_data = from_shaped_array(tile_data.variable_data)
        original_b03_data = ds['B03'].values
        self.assertEqual(original_b03_data[0][2][3], variable_data[0][2][3][0])

        # print(latitudes)
        # print(longitudes)
        # print(variable_data)
        return

    def test_04(self):
        self.assertRaises(RuntimeError, GridMultiVariableReadingProcessor, [], 'lat', 'lon', time='time')
        return


class TestCalendars(unittest.TestCase):
    """
    Test that various calendars can be ingested into SDAP without error.
    """
    def assert_time_data(self, granule_name, time_var, lat_var, lon_var, data_vars):
        granule_path = path.join(
            path.dirname(__file__),
            '../granules/',
            granule_name
        )
        dimensions_to_slices = {
            time_var: slice(0, 1),
            lat_var: slice(0, 30),
            lon_var: slice(0, 30)
        }

        tile = nexusproto.NexusTile()

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            reading_processor = GridMultiVariableReadingProcessor(
                data_vars,
                lat_var,
                lon_var,
                time=time_var
            )
            tile = reading_processor._generate_tile(ds, dimensions_to_slices, tile)
            assert tile.tile.grid_multi_variable_tile.time

    def test_julian_calendar_tile(self):
        self.assert_time_data(
            granule_name='3B-DAY-E.MS.MRG.3IMERG.20070101-S000000-E235959.V06.nc4',
            time_var='time',
            lat_var='lat',
            lon_var='lon',
            data_vars=['HQprecipitation', 'HQprecipitation_cnt']
        )

    def test_gregorian_calendar_tile(self):
        self.assert_time_data(
            granule_name='20190630_d-ACRI-L4-CHL-MULTI_4KM-GLO-REP.nc',
            time_var='time',
            lat_var='lat',
            lon_var='lon',
            data_vars=['CHL', 'CHL_error']
        )

    def test_standard_calendar_tile(self):
        self.assert_time_data(
            granule_name='OISSS_L4_multimission_global_7d_v1.0_2021-03-12.nc',
            time_var='time',
            lat_var='latitude',
            lon_var='longitude',
            data_vars=['sss', 'sss_uncertainty']
        )

    def test_missing_calendar_tile(self):
        self.assert_time_data(
            granule_name='20181231090000-JPL-L4_GHRSST-SSTfnd-MUR25-GLOB-v02.0-fv04.2.nc',
            time_var='time',
            lat_var='lat',
            lon_var='lon',
            data_vars=['analysed_sst', 'analysis_error']
        )


if __name__ == '__main__':
    unittest.main()
