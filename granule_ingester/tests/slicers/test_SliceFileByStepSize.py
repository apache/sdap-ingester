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

from granule_ingester.slicers.SliceFileByStepSize import SliceFileByStepSize


class TestSliceFileByStepSize(unittest.TestCase):

    def test_generate_slices(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/THETA_199201.nc')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            dimension_steps = {'nv': 2, 'time': 1, 'latitude': 180, 'longitude': 180, 'depth': 2}
            slicer = SliceFileByStepSize(dimension_step_sizes=dimension_steps)
            slices = slicer._generate_slices(dimension_specs=dataset.dims)
            expected_slices = [
                'depth:0:2,latitude:0:180,longitude:0:180,nv:0:2,time:0:1',
                'depth:0:2,latitude:0:180,longitude:180:360,nv:0:2,time:0:1',
                'depth:0:2,latitude:0:180,longitude:360:540,nv:0:2,time:0:1',
                'depth:0:2,latitude:0:180,longitude:540:720,nv:0:2,time:0:1',
                'depth:0:2,latitude:180:360,longitude:0:180,nv:0:2,time:0:1',
                'depth:0:2,latitude:180:360,longitude:180:360,nv:0:2,time:0:1',
                'depth:0:2,latitude:180:360,longitude:360:540,nv:0:2,time:0:1',
                'depth:0:2,latitude:180:360,longitude:540:720,nv:0:2,time:0:1',
                'depth:2:4,latitude:0:180,longitude:0:180,nv:0:2,time:0:1',
                'depth:2:4,latitude:0:180,longitude:180:360,nv:0:2,time:0:1',
                'depth:2:4,latitude:0:180,longitude:360:540,nv:0:2,time:0:1',
                'depth:2:4,latitude:0:180,longitude:540:720,nv:0:2,time:0:1',
                'depth:2:4,latitude:180:360,longitude:0:180,nv:0:2,time:0:1',
                'depth:2:4,latitude:180:360,longitude:180:360,nv:0:2,time:0:1',
                'depth:2:4,latitude:180:360,longitude:360:540,nv:0:2,time:0:1',
                'depth:2:4,latitude:180:360,longitude:540:720,nv:0:2,time:0:1'
            ]

            self.assertEqual(expected_slices, slices)

    def test_generate_slices_indexed(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/SMAP_L2B_SSS_04892_20160101T005507_R13080.h5')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            dimension_steps = {'phony_dim_0': 76, 'phony_dim_1': 812, 'phony_dim_2': 1}
            slicer = SliceFileByStepSize(dimension_step_sizes=dimension_steps)
            slices = slicer._generate_slices(dimension_specs=dataset.dims)
            expected_slices = [
                'phony_dim_0:0:76,phony_dim_1:0:812,phony_dim_2:0:1',
                'phony_dim_0:0:76,phony_dim_1:0:812,phony_dim_2:1:2',
                'phony_dim_0:0:76,phony_dim_1:0:812,phony_dim_2:2:3',
                'phony_dim_0:0:76,phony_dim_1:0:812,phony_dim_2:3:4',
                'phony_dim_0:0:76,phony_dim_1:812:1624,phony_dim_2:0:1',
                'phony_dim_0:0:76,phony_dim_1:812:1624,phony_dim_2:1:2',
                'phony_dim_0:0:76,phony_dim_1:812:1624,phony_dim_2:2:3',
                'phony_dim_0:0:76,phony_dim_1:812:1624,phony_dim_2:3:4'
            ]

            self.assertEqual(slices, expected_slices)

    def test_generate_chunk_boundary_slices(self):
        dimension_specs = {'time': 5832, 'rivid': 43}
        dimension_steps = {'time': 2916, 'rivid': 5}
        slicer = SliceFileByStepSize(dimension_step_sizes=dimension_steps)
        boundary_slices = slicer._generate_chunk_boundary_slices(dimension_specs)
        expected_slices = [
            'time:0:2916,rivid:0:5',
            'time:0:2916,rivid:5:10',
            'time:0:2916,rivid:10:15',
            'time:0:2916,rivid:15:20',
            'time:0:2916,rivid:20:25',
            'time:0:2916,rivid:25:30',
            'time:0:2916,rivid:30:35',
            'time:0:2916,rivid:35:40',
            'time:0:2916,rivid:40:43',
            'time:2916:5832,rivid:0:5',
            'time:2916:5832,rivid:5:10',
            'time:2916:5832,rivid:10:15',
            'time:2916:5832,rivid:15:20',
            'time:2916:5832,rivid:20:25',
            'time:2916:5832,rivid:25:30',
            'time:2916:5832,rivid:30:35',
            'time:2916:5832,rivid:35:40',
            'time:2916:5832,rivid:40:43',
        ]

        self.assertEqual(boundary_slices, expected_slices)

    def test_generate_chunk_boundary_slices_indexed(self):
        dimension_steps = {'phony_dim_0': 4, 'phony_dim_1': 4, 'phony_dim_2': 3}
        dimension_specs = {'phony_dim_0': 8, 'phony_dim_1': 8, 'phony_dim_2': 5}
        slicer = SliceFileByStepSize(dimension_step_sizes=dimension_steps)
        boundary_slices = slicer._generate_slices(dimension_specs)
        expected_slices = [
            'phony_dim_0:0:4,phony_dim_1:0:4,phony_dim_2:0:3',
            'phony_dim_0:0:4,phony_dim_1:0:4,phony_dim_2:3:5',
            'phony_dim_0:0:4,phony_dim_1:4:8,phony_dim_2:0:3',
            'phony_dim_0:0:4,phony_dim_1:4:8,phony_dim_2:3:5',
            'phony_dim_0:4:8,phony_dim_1:0:4,phony_dim_2:0:3',
            'phony_dim_0:4:8,phony_dim_1:0:4,phony_dim_2:3:5',
            'phony_dim_0:4:8,phony_dim_1:4:8,phony_dim_2:0:3',
            'phony_dim_0:4:8,phony_dim_1:4:8,phony_dim_2:3:5',
        ]

        self.assertEqual(boundary_slices, expected_slices)


if __name__ == '__main__':
    unittest.main()
