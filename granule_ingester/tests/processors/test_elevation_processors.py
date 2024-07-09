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

from granule_ingester.processors.ElevationBounds import ElevationBounds
from granule_ingester.processors.ElevationOffset import ElevationOffset
from granule_ingester.processors.ElevationRange import ElevationRange
from granule_ingester.processors.reading_processors import GridReadingProcessor


class TestElevationProcessors(unittest.TestCase):

    def read_tiles(self):
        reading_processor = GridReadingProcessor(
            'data_array',
            'latitude',
            'longitude',
            time='time',
            height='elevation'
        )
        granule_path = path.join(path.dirname(__file__), '../granules/dummy_3d_gridded_granule.nc')

        input_tiles = [nexusproto.NexusTile(), nexusproto.NexusTile(), nexusproto.NexusTile()]

        for it in input_tiles:
            it.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'latitude': slice(15, 20),
            'longitude': slice(0, 5),
        }

        with xr.open_dataset(granule_path) as ds:
            output_tiles = []

            for i in range(len(input_tiles)):
                dimensions_to_slices['elevation'] = slice(i, i + 1)

                section_spec = ','.join([':'.join([dim, str(dimensions_to_slices[dim].start), str(dimensions_to_slices[dim].stop)]) for dim in dimensions_to_slices])
                input_tiles[i].summary.section_spec = section_spec

                output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tiles[i])

                self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
                self.assertEqual(int(datetime(2023, 6, 12, tzinfo=timezone.utc).timestamp()), output_tile.tile.grid_tile.time)
                self.assertEqual([5, 5], output_tile.tile.grid_tile.variable_data.shape)
                self.assertEqual([5], output_tile.tile.grid_tile.latitude.shape)
                self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)
                self.assertEqual([5, 5], output_tile.tile.grid_tile.elevation.shape)
                self.assertEqual(10 + (10 * i), output_tile.tile.grid_tile.min_elevation)
                self.assertEqual(10 + (10 * i), output_tile.tile.grid_tile.max_elevation)

                output_tiles.append(output_tile)

            return output_tiles, ds

    def read_tiles_no_coord(self):
        reading_processor = GridReadingProcessor(
            'data_array',
            'latitude',
            'longitude',
            time='time',
        )
        granule_path = path.join(path.dirname(__file__), '../granules/dummy_3d_gridded_granule_no_coord.nc')

        input_tiles = [nexusproto.NexusTile(), nexusproto.NexusTile(), nexusproto.NexusTile()]

        for it in input_tiles:
            it.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'latitude': slice(15, 20),
            'longitude': slice(0, 5),
        }

        with xr.open_dataset(granule_path) as ds:
            output_tiles = []

            for i in range(len(input_tiles)):
                dimensions_to_slices['elevation'] = slice(i, i + 1)

                section_spec = ','.join([':'.join([dim, str(dimensions_to_slices[dim].start), str(dimensions_to_slices[dim].stop)]) for dim in dimensions_to_slices])
                input_tiles[i].summary.section_spec = section_spec

                output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tiles[i])

                self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
                self.assertEqual(int(datetime(2023, 6, 12, tzinfo=timezone.utc).timestamp()), output_tile.tile.grid_tile.time)
                self.assertEqual([5, 5], output_tile.tile.grid_tile.variable_data.shape)
                self.assertEqual([5], output_tile.tile.grid_tile.latitude.shape)
                self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)

                # Elevation data should be unset because it is not properly available in source granule
                self.assertEqual([], output_tile.tile.grid_tile.elevation.shape)
                self.assertEqual(0, output_tile.tile.grid_tile.min_elevation)
                self.assertEqual(0, output_tile.tile.grid_tile.max_elevation)

                output_tiles.append(output_tile)

            return output_tiles, ds

    def test_elevation_offset(self):
        tiles, ds = self.read_tiles()

        expected_subset_elevation_array = np.broadcast_to(np.arange(15, 20).reshape((5,1)), (5,5))

        processor = ElevationOffset('elevation_array', 'elevation')

        for i in range(len(tiles)):
            tile = tiles[i]

            expected_elevation_offset = 10 + (10 * i)
            expected_elevation_array = expected_subset_elevation_array + expected_elevation_offset

            self.assertEqual(expected_elevation_offset, tile.tile.grid_tile.min_elevation)

            processed_tile = processor.process(tile, ds)

            self.assertTrue(
                np.array_equal(expected_elevation_array, from_shaped_array(processed_tile.tile.grid_tile.elevation)),
                f'{expected_elevation_array} != {from_shaped_array(processed_tile.tile.grid_tile.elevation)}'
            )

            self.assertEqual(np.min(expected_elevation_array), processed_tile.tile.grid_tile.min_elevation)
            self.assertEqual(np.max(expected_elevation_array), processed_tile.tile.grid_tile.max_elevation)

    def test_elevation_bounds(self):
        tiles, ds = self.read_tiles()

        expected_elevations = [10, 20, 30]
        expected_min_max = [[5, 15], [15, 25], [25, 35]]

        processor = ElevationBounds('elevation', 'z_bnds')

        for i in range(len(tiles)):
            tile = tiles[i]

            self.assertEqual(expected_elevations[i], tile.tile.grid_tile.min_elevation)

            processed_tile = processor.process(tile, ds)
            elevation_array = from_shaped_array(processed_tile.tile.grid_tile.elevation)

            self.assertTrue(all(elevation_array.flatten() == expected_elevations[i]), f'{elevation_array} != {expected_elevations[i]}')

            self.assertEqual(expected_min_max[i][0], processed_tile.tile.grid_tile.min_elevation)
            self.assertEqual(expected_min_max[i][1], processed_tile.tile.grid_tile.max_elevation)

    def test_elevaton_range(self):
        tiles, ds = self.read_tiles_no_coord()

        expected_elevations = [10, 20, 30]

        processor = ElevationRange('elevation', 10, 30, 10)

        for i in range(len(tiles)):
            tile = tiles[i]

            processed_tile = processor.process(tile, ds)
            elevation_array = from_shaped_array(processed_tile.tile.grid_tile.elevation)

            self.assertTrue(all(elevation_array.flatten() == expected_elevations[i]), f'{elevation_array} != {expected_elevations[i]}')

            self.assertEqual(expected_elevations[i], processed_tile.tile.grid_tile.min_elevation)
            self.assertEqual(expected_elevations[i], processed_tile.tile.grid_tile.max_elevation)
