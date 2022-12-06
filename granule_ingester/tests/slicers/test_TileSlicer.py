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

import asyncio
import os
import unittest
from granule_ingester.slicers.TileSlicer import TileSlicer
import xarray as xr


class TestTileSlicer(unittest.TestCase):
    class ToyTileSlicer(TileSlicer):
        def _generate_slices(self, dimensions):
            return [
                'time:0:1,lat:0:4,lon:0:4',
                'time:1:2,lat:0:4,lon:0:4',
                'time:2:3,lat:0:4,lon:0:4',

                'time:0:1,lat:0:4,lon:4:8',
                'time:1:2,lat:0:4,lon:4:8',
                'time:2:3,lat:0:4,lon:4:8',

                'time:0:1,lat:4:8,lon:0:4',
                'time:1:2,lat:4:8,lon:0:4',
                'time:2:3,lat:4:8,lon:0:4',

                'time:0:1,lat:4:8,lon:4:8',
                'time:1:2,lat:4:8,lon:4:8',
                'time:2:3,lat:4:8,lon:4:8'
            ]

    def test_generate_tiles(self):
        relative_path = '../granules/20050101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
        file_path = os.path.join(os.path.dirname(__file__), relative_path)
        with xr.open_dataset(file_path) as dataset:
            slicer = TestTileSlicer.ToyTileSlicer().generate_tiles(dataset, file_path)

        expected_slices = slicer._generate_slices(None)
        self.assertEqual(file_path, slicer._granule_name)
        self.assertEqual(expected_slices, slicer._tile_spec_list)

    # def test_open_s3(self):
    #     s3_path = 's3://nexus-ingest/avhrr/198109-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
    #     slicer = TestTileSlicer.ToyTileSlicer(resource=s3_path)
    #
    #     expected_slices = slicer._generate_slices(None)
    #     asyncio.run(slicer.open())
    #     self.assertIsNotNone(slicer.dataset)
    #     self.assertEqual(expected_slices, slicer._tile_spec_list)

    def test_next(self):
        relative_path = '../granules/20050101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
        file_path = os.path.join(os.path.dirname(__file__), relative_path)
        with xr.open_dataset(file_path) as dataset:
            slicer = TestTileSlicer.ToyTileSlicer().generate_tiles(dataset, file_path)
        generated_tiles = list(slicer)

        expected_slices = slicer._generate_slices(None)
        self.assertListEqual(expected_slices, [tile.summary.section_spec for tile in generated_tiles])
        for tile in generated_tiles:
            self.assertEqual(file_path, tile.summary.granule)

    # def test_download_s3_file(self):
    #     slicer = TestTileSlicer.ToyTileSlicer(resource=None)
    #
    #     asyncio.run(slicer._download_s3_file(
    #         "s3://nexus-ingest/avhrr/198109-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc"))


if __name__ == '__main__':
    unittest.main()
