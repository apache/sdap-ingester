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

from granule_ingester.processors.reading_processors import TimeSeriesReadingProcessor


class TestReadWSWMData(unittest.TestCase):

    def test_read_not_empty_wswm(self):
        reading_processor = TimeSeriesReadingProcessor('Qout', 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_wswm.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 5832),
            'rivid': slice(0, 1),
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual([5832], output_tile.tile.time_series_tile.time.shape)
            self.assertEqual([5832, 1], output_tile.tile.time_series_tile.variable_data.shape)
            self.assertEqual([1], output_tile.tile.time_series_tile.latitude.shape)
            self.assertEqual([1], output_tile.tile.time_series_tile.longitude.shape)

        # test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_wswm.nc')
        # wswm_reader = TimeSeriesReadingProcessor('Qout', 'lat', 'lon', 'time')
        #
        # input_tile = nexusproto.NexusTile()
        # tile_summary = nexusproto.TileSummary()
        # tile_summary.granule = "file:%s" % test_file
        # tile_summary.section_spec = "time:0:5832,rivid:0:1"
        # input_tile.summary.CopyFrom(tile_summary)
        #
        # results = list(wswm_reader.process(input_tile))
        #
        # self.assertEqual(1, len(results))
        #
        # for nexus_tile in results:
        #     self.assertTrue(nexus_tile.HasField('tile'))
        #     self.assertTrue(nexus_tile.tile.HasField('time_series_tile'))
        #
        #     tile = nexus_tile.tile.time_series_tile
        #     self.assertEqual(1, from_shaped_array(tile.latitude).size)
        #     self.assertEqual(1, from_shaped_array(tile.longitude).size)
        #     self.assertEqual((5832, 1), from_shaped_array(tile.variable_data).shape)
        #
        # tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.variable_data))
        # self.assertEqual(5832, np.ma.count(tile1_data))
        # self.assertAlmostEqual(45.837,
        #                        np.ma.min(
        #                            np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.latitude))),
        #                        places=3)
        # self.assertAlmostEqual(-122.789,
        #                        np.ma.max(
        #                            np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.longitude))),
        #                        places=3)
        #
        # tile1_times = from_shaped_array(results[0].tile.time_series_tile.time)
        # self.assertEqual(852098400, tile1_times[0])
        # self.assertEqual(915073200, tile1_times[-1])
        # self.assertAlmostEqual(1.473,
        #                        np.ma.masked_invalid(from_shaped_array(results[0].tile.time_series_tile.variable_data))[
        #                            0, 0],
        #                        places=3)
