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
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array

from granule_ingester.processors.reading_processors import GridReadingProcessor


class TestReadMurData(unittest.TestCase):

    def test_read_empty_mur(self):
        reading_processor = GridReadingProcessor('analysed_sst', 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 10),
            'lon': slice(0, 5)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1451638800, output_tile.tile.grid_tile.time)
            self.assertEqual([10, 5], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([10], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(0, np.ma.count(masked_data))

    def test_read_empty_mur_01(self):
        reading_processor = GridReadingProcessor(['analysed_sst'], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 10),
            'lon': slice(0, 5)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1451638800, output_tile.tile.grid_tile.time)
            self.assertEqual([10, 5], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([10], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(0, np.ma.count(masked_data))
        return

    def test_read_not_empty_mur(self):
        reading_processor = GridReadingProcessor('analysed_sst', 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 10),
            'lon': slice(0, 5)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1451638800, output_tile.tile.grid_tile.time)
            self.assertEqual([10, 5], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([10], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(50, np.ma.count(masked_data))

    def test_read_not_empty_mur_01(self):
        reading_processor = GridReadingProcessor(['analysed_sst'], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_mur.nc4')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 10),
            'lon': slice(0, 5)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1451638800, output_tile.tile.grid_tile.time)
            self.assertEqual([10, 5], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([10], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(50, np.ma.count(masked_data))


class TestReadCcmpData(unittest.TestCase):

    def test_read_not_empty_ccmp(self):
        reading_processor = GridReadingProcessor('uwnd', 'latitude', 'longitude', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_ccmp.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'latitude': slice(0, 38),
            'longitude': slice(0, 87)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1451606400, output_tile.tile.grid_tile.time)
            self.assertEqual([38, 87], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([38], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([87], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(3306, np.ma.count(masked_data))

        # test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_ccmp.nc')
        #
        # ccmp_reader = GridReadingProcessor('uwnd', 'latitude', 'longitude', time='time', meta='vwnd')
        #
        # input_tile = nexusproto.NexusTile()
        # tile_summary = nexusproto.TileSummary()
        # tile_summary.granule = "file:%s" % test_file
        # tile_summary.section_spec = "time:0:1,longitude:0:87,latitude:0:38"
        # input_tile.summary.CopyFrom(tile_summary)
        #
        # results = list(ccmp_reader.process(input_tile))
        #
        # self.assertEqual(1, len(results))
        #
        # # with open('./ccmp_nonempty_nexustile.bin', 'w') as f:
        # #     f.write(results[0])
        #
        # for nexus_tile in results:
        #     self.assertTrue(nexus_tile.HasField('tile'))
        #     self.assertTrue(nexus_tile.tile.HasField('grid_tile'))
        #     self.assertEqual(1, len(nexus_tile.tile.grid_tile.meta_data))
        #
        #     tile = nexus_tile.tile.grid_tile
        #     self.assertEqual(38, from_shaped_array(tile.latitude).size)
        #     self.assertEqual(87, from_shaped_array(tile.longitude).size)
        #     self.assertEqual((1, 38, 87), from_shaped_array(tile.variable_data).shape)
        #
        # tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))
        # self.assertEqual(3306, np.ma.count(tile1_data))
        # self.assertAlmostEqual(-78.375,
        #                        np.ma.min(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
        #                        places=3)
        # self.assertAlmostEqual(-69.125,
        #                        np.ma.max(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
        #                        places=3)
        #
        # self.assertEqual(1451606400, results[0].tile.grid_tile.time)


class TestReadAvhrrData(unittest.TestCase):
    def test_read_not_empty_avhrr(self):
        reading_processor = GridReadingProcessor('analysed_sst', 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/not_empty_avhrr.nc4')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 5),
            'lon': slice(0, 10)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1462060800, output_tile.tile.grid_tile.time)
            self.assertEqual([5, 10], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([10], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(50, np.ma.count(masked_data))
        # test_file = path.join(path.dirname(__file__), 'datafiles', 'not_empty_avhrr.nc4')
        #
        # avhrr_reader = GridReadingProcessor('analysed_sst', 'lat', 'lon', time='time')
        #
        # input_tile = nexusproto.NexusTile()
        # tile_summary = nexusproto.TileSummary()
        # tile_summary.granule = "file:%s" % test_file
        # tile_summary.section_spec = "time:0:1,lat:0:10,lon:0:10"
        # input_tile.summary.CopyFrom(tile_summary)
        #
        # results = list(avhrr_reader.process(input_tile))
        #
        # self.assertEqual(1, len(results))
        #
        # for nexus_tile in results:
        #     self.assertTrue(nexus_tile.HasField('tile'))
        #     self.assertTrue(nexus_tile.tile.HasField('grid_tile'))
        #
        #     tile = nexus_tile.tile.grid_tile
        #     self.assertEqual(10, from_shaped_array(tile.latitude).size)
        #     self.assertEqual(10, from_shaped_array(tile.longitude).size)
        #     self.assertEqual((1, 10, 10), from_shaped_array(tile.variable_data).shape)
        #
        # tile1_data = np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))
        # self.assertEqual(100, np.ma.count(tile1_data))
        # self.assertAlmostEqual(-39.875,
        #                        np.ma.min(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
        #                        places=3)
        # self.assertAlmostEqual(-37.625,
        #                        np.ma.max(np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.latitude))),
        #                        places=3)
        #
        # self.assertEqual(1462060800, results[0].tile.grid_tile.time)
        # self.assertAlmostEqual(289.71,
        #                        np.ma.masked_invalid(from_shaped_array(results[0].tile.grid_tile.variable_data))[
        #                            0, 0, 0],
        #                        places=3)


class TestReadInterpEccoData(unittest.TestCase):
    def setUp(self):
        self.module = GridReadingProcessor('OBP', 'latitude', 'longitude', x_dim='i', y_dim='j',
                                           time='time')

    def test_read_indexed_ecco(self):
        reading_processor = GridReadingProcessor(variable='OBP',
                                                 latitude='latitude',
                                                 longitude='longitude',
                                                 time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/OBP_2017_01.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'j': slice(0, 5),
            'i': slice(0, 10)
        }
        with xr.open_dataset(granule_path) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

            self.assertEqual(granule_path, output_tile.summary.granule, granule_path)
            self.assertEqual(1484568000, output_tile.tile.grid_tile.time)
            self.assertEqual([5, 10], output_tile.tile.grid_tile.variable_data.shape)
            self.assertEqual([5], output_tile.tile.grid_tile.latitude.shape)
            self.assertEqual([10], output_tile.tile.grid_tile.longitude.shape)

            masked_data = np.ma.masked_invalid(from_shaped_array(output_tile.tile.grid_tile.variable_data))
            self.assertEqual(50, np.ma.count(masked_data))

        # test_file = path.join(path.dirname(__file__), 'datafiles', 'OBP_2017_01.nc')
        #
        # input_tile = nexusproto.NexusTile()
        # tile_summary = nexusproto.TileSummary()
        # tile_summary.granule = "file:%s" % test_file
        # tile_summary.section_spec = "time:0:1,j:0:10,i:0:10"
        # input_tile.summary.CopyFrom(tile_summary)
        #
        # results = list(self.module.process(input_tile))
        #
        # self.assertEqual(1, len(results))
        #
        # for nexus_tile in results:
        #     self.assertTrue(nexus_tile.HasField('tile'))
        #     self.assertTrue(nexus_tile.tile.HasField('grid_tile'))
        #
        #     tile = nexus_tile.tile.grid_tile
        #     self.assertEqual(10, len(from_shaped_array(tile.latitude)))
        #     self.assertEqual(10, len(from_shaped_array(tile.longitude)))
        #
        #     the_data = np.ma.masked_invalid(from_shaped_array(tile.variable_data))
        #     self.assertEqual((1, 10, 10), the_data.shape)
        #     self.assertEqual(100, np.ma.count(the_data))
        #     self.assertEqual(1484568000, tile.time)


class TestReadHLSData(unittest.TestCase):
    def test_01(self):
        reading_processor = GridReadingProcessor([f'B{k:02d}' for k in range(1, 12)], 'lat', 'lon', time='time')
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

    def test_02(self):
        reading_processor = GridReadingProcessor('B03', 'lat', 'lon', time='time')
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

        print(latitudes)
        print(longitudes)
        print(variable_data)
        return

    def test_03(self):
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

        print(latitudes)
        print(longitudes)
        print(variable_data)
        return

    def test_04(self):
        reading_processor = GridReadingProcessor([], 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30)
        }

        with xr.open_dataset(granule_path) as ds:
            self.assertRaises(ValueError, reading_processor._generate_tile, ds, dimensions_to_slices, input_tile)
        return


if __name__ == '__main__':
    unittest.main()
