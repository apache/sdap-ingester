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
from datetime import datetime, timezone
from os import path

import numpy as np
import xarray as xr
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array

from granule_ingester.processors.reading_processors import GridReadingProcessor


# a = np.arange(0, 51*51).reshape((51, 51))
# da = np.array([a, a+1000, a+2000])
#
# xr.Dataset(
#     data_vars=dict(
#         data_array=(['time', 'elevation', 'latitude', 'longitude'], np.array([da])),
#         elevation_array=(['latitude', 'longitude'], np.broadcast_to(np.arange(51).reshape((51, 1)), (51, 51))),
#         z_bnds=(['elevation', 'nb'], np.array([[5,15],[15,25],[25,35]]))
#     ),
#     coords=dict(
#         latitude=(['latitude'], np.arange(-25,26)),
#         longitude=(['longitude'], np.arange(51)),
#         elevation=(['elevation'], np.arange(10,40,10)),
#         time=(['time'], np.array([datetime(2023, 6, 12, 0, 0, 0)], dtype='datetime64[ns]'))
#     )
# )
#
#


class TestRead3dData(unittest.TestCase):
    """
    Testing with gridded singlevar data for now
    """

    def test_read_elevation_array(self):
        reading_processor = GridReadingProcessor(
            'data_array',
            'latitude',
            'longitude',
            time='time',
            height='elevation'
        )
        granule_path = path.join(path.dirname(__file__), '../granules/dummy_3d_gridded_granule.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'latitude': slice(15, 20),
            'longitude': slice(0, 5),
            'elevation': slice(1, 2)
        }

        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(int(datetime(2023, 6, 12, tzinfo=timezone.utc).timestamp()), output_tile.tile.grid_tile.time)
            self.assertEqual([5, 5], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)
            self.assertEqual([5, 5], output_tile.tile.grid_tile.elevation.shape)
            self.assertEqual(20, output_tile.tile.grid_tile.min_elevation)
            self.assertEqual(20, output_tile.tile.grid_tile.max_elevation)

            elevation_array = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.elevation))

            self.assertTrue(all(elevation_array.flatten() == 20))

