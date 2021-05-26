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
from nexusproto.serialization import from_shaped_array

from granule_ingester.processors.reading_processors import BandedGridReadingProcessor


class TestReadHLSData(unittest.TestCase):
    def test_01(self):
        reading_processor = BandedGridReadingProcessor([f'B{k:02d}' for k in range(1, 12)], 'lat', 'lon', time='time')
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
        self.assertEqual([30, 30, 11], generated_tile.tile.grid_tile.variable_data.shape)
        self.assertEqual([30], generated_tile.tile.grid_tile.latitude.shape)
        self.assertEqual([30], generated_tile.tile.grid_tile.longitude.shape)
        
        print(latitudes)
        print(longitudes)
        print(variable_data)
        return


if __name__ == '__main__':
    unittest.main()
