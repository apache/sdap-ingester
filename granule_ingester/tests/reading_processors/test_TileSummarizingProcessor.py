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

import xarray as xr
from granule_ingester.processors import TileSummarizingProcessor
from granule_ingester.processors.reading_processors import GridMultiVariableReadingProcessor
from granule_ingester.processors.reading_processors.GridReadingProcessor import GridReadingProcessor
from nexusproto import DataTile_pb2 as nexusproto


class TestTileSummarizingProcessor(unittest.TestCase):
    def test_standard_name_exists_01(self):
        """
        Test that the standard_name attribute exists in a
        Tile.TileSummary object after being processed with
        TileSummarizingProcessor
        """
        reading_processor = GridReadingProcessor(
            variable='analysed_sst',
            latitude='lat',
            longitude='lon',
            time='time',
            tile='tile'
        )
        relative_path = '../granules/20050101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
        granule_path = path.join(path.dirname(__file__), relative_path)
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path
        tile_summary.data_var_name = json.dumps('analysed_sst')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dims = {
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            'time': slice(0, 1),
            'tile': slice(10, 11),
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dims, input_tile)
            tile_summary_processor = TileSummarizingProcessor('test')
            new_tile = tile_summary_processor.process(tile=output_tile, dataset=ds)
            self.assertEqual('"sea_surface_temperature"', new_tile.summary.standard_name, f'wrong new_tile.summary.standard_name')

    def test_hls_single_var01(self):
        """
        Test that the standard_name attribute exists in a
        Tile.TileSummary object after being processed with
        TileSummarizingProcessor
        """
        input_var_list = [f'B{k:02d}' for k in range(1, 12)]
        input_var_list = ['B01']
        reading_processor = GridReadingProcessor(input_var_list, 'lat', 'lon', time='time', tile='tile')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path
        tile_summary.data_var_name = json.dumps(input_var_list)

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            'tile': slice(10, 11),
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            tile_summary_processor = TileSummarizingProcessor('test')
            new_tile = tile_summary_processor.process(tile=output_tile, dataset=ds)
            self.assertEqual('null', new_tile.summary.standard_name, f'wrong new_tile.summary.standard_name')
            self.assertEqual(None, json.loads(new_tile.summary.standard_name), f'unable to convert new_tile.summary.standard_name from JSON')
            self.assertTrue(abs(new_tile.summary.stats.mean - 0.26137) < 0.001, f'mean value is not close expected: 0.26137. actual: {new_tile.summary.stats.mean}')

    def test_hls_multiple_var_01(self):
        """
        Test that the standard_name attribute exists in a
        Tile.TileSummary object after being processed with
        TileSummarizingProcessor
        """
        input_var_list = [f'B{k:02d}' for k in range(1, 12)]
        reading_processor = GridMultiVariableReadingProcessor(input_var_list, 'lat', 'lon', time='time', tile='tile')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path
        tile_summary.data_var_name = json.dumps(input_var_list)

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            'tile': slice(10, 11),
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            tile_summary_processor = TileSummarizingProcessor('test')
            new_tile = tile_summary_processor.process(tile=output_tile, dataset=ds)
            self.assertEqual('[null, null, null, null, null, null, null, null, null, null, null]', new_tile.summary.standard_name, f'wrong new_tile.summary.standard_name')
            self.assertEqual([None for _ in range(11)], json.loads(new_tile.summary.standard_name), f'unable to convert new_tile.summary.standard_name from JSON')
            self.assertTrue(abs(new_tile.summary.stats.mean - 0.26523) < 0.001, f'mean value is not close expected: 0.26523. actual: {new_tile.summary.stats.mean}')