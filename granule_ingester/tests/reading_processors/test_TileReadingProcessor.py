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
from collections import OrderedDict
from os import path

import xarray as xr

from granule_ingester.processors.reading_processors import TileReadingProcessor


class TestEccoReadingProcessor(unittest.TestCase):

    def test_slices_for_variable(self):
        dimensions_to_slices = {
            'j': slice(0, 1),
            'tile': slice(0, 1),
            'i': slice(0, 1),
            'time': slice(0, 1)
        }

        expected = {
            'tile': slice(0, 1, None),
            'j': slice(0, 1, None),
            'i': slice(0, 1, None)
        }

        granule_path = path.join(path.dirname(__file__), '../granules/OBP_native_grid.nc')
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            slices = TileReadingProcessor._slices_for_variable(ds['XC'], dimensions_to_slices)
            self.assertEqual(slices, expected)
