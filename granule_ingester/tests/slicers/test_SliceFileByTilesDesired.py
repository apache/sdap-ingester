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

# import unittest
# from collections import Set
#
# from netCDF4 import Dataset
# from granule_ingester.slicers.SliceFileByTilesDesired import SliceFileByTilesDesired
#
#
# class TestSliceFileByTilesDesired(unittest.TestCase):
#
#     def test_generate_slices(self):
#         netcdf_path = 'tests/granules/20050101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
#         dataset = Dataset(netcdf_path)
#         dimension_specs = {value.name: value.size for key,
#                            value in dataset.dimensions.items()}
#
#         slicer = SliceFileByTilesDesired(tiles_desired=2,
#                                          desired_spatial_dimensions=['lat', 'lon'])
#         slices = slicer.generate_slices(dimension_specs)
#         expected_slices = ['lat:0:509,lon:0:1018',
#                            'lat:0:509,lon:1018:1440',
#                            'lat:509:720,lon:0:1018',
#                            'lat:509:720,lon:1018:1440']
#         self.assertEqual(slices, expected_slices)
#
#     def test_generate_slices_with_time(self):
#         netcdf_path = 'tests/granules/20050101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
#         dataset = Dataset(netcdf_path)
#         dimension_specs = {value.name: value.size for key,
#                            value in dataset.dimensions.items()}
#
#         slicer = SliceFileByTilesDesired(tiles_desired=2,
#                                          desired_spatial_dimensions=[
#                                              'lat', 'lon'],
#                                          time_dimension=('time', 3))
#         slices = slicer.generate_slices(dimension_specs)
#         expected_slices = ['time:0:1,lat:0:509,lon:0:1018',
#                            'time:1:2,lat:0:509,lon:0:1018',
#
#                            'time:0:1,lat:0:509,lon:1018:1440',
#                            'time:1:2,lat:0:509,lon:1018:1440',
#
#                            'time:0:1,lat:509:720,lon:0:1018',
#                            'time:1:2,lat:509:720,lon:0:1018',
#
#                            'time:0:1,lat:509:720,lon:1018:1440',
#                            'time:1:2,lat:509:720,lon:1018:1440']
#         self.assertEqual(slices, expected_slices)
#
#     def test_calculate_step_size_perfect_split_2dim(self):
#         step_size = SliceFileByTilesDesired._calculate_step_size(1000, 100, 2)
#         self.assertAlmostEqual(step_size, 100.0)
#
#     def test_calculate_step_size_perfect_split_3dim(self):
#         step_size = SliceFileByTilesDesired._calculate_step_size(1000, 100, 3)
#         self.assertAlmostEqual(step_size, 215.0)
#
#     def test_generate_spatial_slices(self):
#         dimension_specs = {'lat': 8, 'lon': 8}
#         slicer = SliceFileByTilesDesired(tiles_desired=2,
#                                          desired_spatial_dimensions=dimension_specs)
#         boundary_slices = slicer._generate_spatial_slices(tiles_desired=4,
#                                                           dimension_specs=dimension_specs)
#         expected_slices = [
#             'lat:0:4,lon:0:4',
#             'lat:0:4,lon:4:8',
#             'lat:4:8,lon:0:4',
#             'lat:4:8,lon:4:8'
#         ]
#         self.assertEqual(boundary_slices, expected_slices)
#
#     def test_generate_temporal_slices(self):
#         slicer = SliceFileByTilesDesired(tiles_desired=2,
#                                          desired_spatial_dimensions=None)
#         time_slices = slicer._generate_temporal_slices(('time', 10))
#         expected_time_slices = ['time:0:1',
#                                 'time:1:2',
#                                 'time:2:3',
#                                 'time:3:4',
#                                 'time:4:5',
#                                 'time:5:6',
#                                 'time:6:7',
#                                 'time:7:8',
#                                 'time:8:9']
#         self.assertEqual(time_slices, expected_time_slices)
#
#
# if __name__ == '__main__':
#     unittest.main()
