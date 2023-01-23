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
# from granule_ingester.slicers.SliceFileByDimension import SliceFileByDimension
#
#
# class TestSliceFileByTilesDesired(unittest.TestCase):
#
#     def test_generate_slices(self):
#         netcdf_path = 'tests/granules/THETA_199201.nc'
#         dataset = Dataset(netcdf_path)
#         dimension_specs = {value.name: value.size for key,
#                            value in dataset.dimensions.items()}
#
#         slicer = SliceFileByDimension(slice_dimension='depth',
#                                       dimension_name_prefix=None)
#         slices = slicer.generate_slices(dimension_specs)
#         expected_slices = ['nv:0:2,time:0:1,longitude:0:720,depth:0:1,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:1:2,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:2:3,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:3:4,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:4:5,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:5:6,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:6:7,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:7:8,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:8:9,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:9:10,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:10:11,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:11:12,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:12:13,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:13:14,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:14:15,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:15:16,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:16:17,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:17:18,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:18:19,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:19:20,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:20:21,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:21:22,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:22:23,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:23:24,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:24:25,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:25:26,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:26:27,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:27:28,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:28:29,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:29:30,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:30:31,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:31:32,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:32:33,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:33:34,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:34:35,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:35:36,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:36:37,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:37:38,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:38:39,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:39:40,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:40:41,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:41:42,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:42:43,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:43:44,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:44:45,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:45:46,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:46:47,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:47:48,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:48:49,latitude:0:360',
#                            'nv:0:2,time:0:1,longitude:0:720,depth:49:50,latitude:0:360']
#
#         self.assertEqual(slices, expected_slices)
#
#     def test_generate_slices_indexed(self):
#         netcdf_path = 'tests/granules/SMAP_L2B_SSS_04892_20160101T005507_R13080.h5'
#         dataset = Dataset(netcdf_path)
#         dimension_specs = {value.name: value.size for key,
#                                                       value in dataset.dimensions.items()}
#
#         slicer = SliceFileByDimension(slice_dimension='2',
#                                       dimension_name_prefix='phony_dim_')
#         slices = slicer.generate_slices(dimension_specs)
#         expected_slices = [
#             'phony_dim_0:0:76,phony_dim_1:0:1624,phony_dim_2:0:1',
#             'phony_dim_0:0:76,phony_dim_1:0:1624,phony_dim_2:1:2',
#             'phony_dim_0:0:76,phony_dim_1:0:1624,phony_dim_2:2:3',
#             'phony_dim_0:0:76,phony_dim_1:0:1624,phony_dim_2:3:4'
#         ]
#
#         self.assertEqual(slices, expected_slices)
#
#     def test_indexed_dimension_slicing(self):
#         # for some reason, python automatically prefixes integer-indexed dimensions with "phony_dim_"
#         dimension_specs = {'phony_dim_0': 8, 'phony_dim_1': 8, 'phony_dim_2': 5}
#         slicer = SliceFileByDimension(slice_dimension='2',
#                                       dimension_name_prefix=None)
#         boundary_slices = slicer._indexed_dimension_slicing(dimension_specs)
#         expected_slices = [
#             'phony_dim_0:0:8,phony_dim_1:0:8,phony_dim_2:0:1',
#             'phony_dim_0:0:8,phony_dim_1:0:8,phony_dim_2:1:2',
#             'phony_dim_0:0:8,phony_dim_1:0:8,phony_dim_2:2:3',
#             'phony_dim_0:0:8,phony_dim_1:0:8,phony_dim_2:3:4',
#             'phony_dim_0:0:8,phony_dim_1:0:8,phony_dim_2:4:5'
#         ]
#
#         self.assertEqual(boundary_slices, expected_slices)
#
#     def test_generate_tile_boundary_slices(self):
#         dimension_specs = {'lat': 8, 'lon': 8, 'depth': 5}
#         slicer = SliceFileByDimension(slice_dimension='depth',
#                                       dimension_name_prefix=None)
#         boundary_slices = slicer._generate_tile_boundary_slices(slicer._slice_by_dimension,dimension_specs)
#         expected_slices = [
#             'lat:0:8,lon:0:8,depth:0:1',
#             'lat:0:8,lon:0:8,depth:1:2',
#             'lat:0:8,lon:0:8,depth:2:3',
#             'lat:0:8,lon:0:8,depth:3:4',
#             'lat:0:8,lon:0:8,depth:4:5'
#         ]
#
#         self.assertEqual(boundary_slices, expected_slices)
#
# if __name__ == '__main__':
#     unittest.main()
