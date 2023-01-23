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

import json
import unittest

import uuid
from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.processors import GenerateTileId


class TestGenerateTileId(unittest.TestCase):

    def test_process_01(self):
        processor = GenerateTileId()

        tile = nexusproto.NexusTile()
        tile.summary.granule = 'test_dir/test_granule.nc'
        tile.summary.data_var_name = json.dumps('test_variable')
        tile.summary.section_spec = 'i:0:90,j:0:90,k:8:9,nv:0:2,tile:4:5,time:8:9'

        expected_id = uuid.uuid3(uuid.NAMESPACE_DNS,
                                 'test_granule.nc' + json.dumps('test_variable') + 'i:0:90,j:0:90,k:8:9,nv:0:2,tile:4:5,time:8:9')

        self.assertEqual(str(expected_id), processor.process(tile).summary.tile_id)

    def test_process_02(self):
        processor = GenerateTileId()

        input_var_list = ['B01', 'B02', 'B03']
        tile = nexusproto.NexusTile()
        tile.summary.granule = 'test_dir/test_granule.nc'
        tile.summary.data_var_name = json.dumps(input_var_list)
        tile.summary.section_spec = 'i:0:90,j:0:90,k:8:9,nv:0:2,tile:4:5,time:8:9'

        expected_id = uuid.uuid3(uuid.NAMESPACE_DNS,
                                 'test_granule.nc' + json.dumps(input_var_list) + 'i:0:90,j:0:90,k:8:9,nv:0:2,tile:4:5,time:8:9')

        self.assertEqual(str(expected_id), processor.process(tile).summary.tile_id)
