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

import os
import unittest

from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.pipeline.Pipeline import Pipeline
from granule_ingester.processors import GenerateTileId
from granule_ingester.processors.reading_processors import EccoReadingProcessor
from granule_ingester.slicers.SliceFileByStepSize import SliceFileByStepSize
from granule_ingester.writers import DataStore, MetadataStore
from granule_ingester.exceptions import PipelineBuildingError


class TestPipeline(unittest.TestCase):
    class MockProcessorNoParams:
        def __init__(self):
            pass

    class MockProcessorWithParams:
        def __init__(self, test_param):
            self.test_param = test_param

    def test_parse_config(self):
        class MockDataStore(DataStore):
            def save_data(self, nexus_tile: nexusproto.NexusTile) -> None:
                pass

        class MockMetadataStore(MetadataStore):
            def save_metadata(self, nexus_tile: nexusproto.NexusTile) -> None:
                pass

        relative_path = "../config_files/ingestion_config_testfile.yaml"
        with open(os.path.join(os.path.dirname(__file__), relative_path)) as file:
            yaml_str = file.read()
        pipeline = Pipeline.from_string(config_str=yaml_str,
                                        data_store_factory=MockDataStore,
                                        metadata_store_factory=MockMetadataStore)

        self.assertEqual(pipeline._data_store_factory, MockDataStore)
        self.assertEqual(pipeline._metadata_store_factory, MockMetadataStore)
        self.assertEqual(type(pipeline._slicer), SliceFileByStepSize)
        self.assertEqual(type(pipeline._tile_processors[0]), EccoReadingProcessor)
        self.assertEqual(type(pipeline._tile_processors[1]), GenerateTileId)

    def test_parse_module(self):
        module_mappings = {
            "sliceFileByStepSize": SliceFileByStepSize
        }

        module_config = {
            "name": "sliceFileByStepSize",
            "dimension_step_sizes": {
                "time": 1,
                "lat": 10,
                "lon": 10
            }
        }
        module = Pipeline._parse_module(module_config, module_mappings)
        self.assertEqual(SliceFileByStepSize, type(module))
        self.assertEqual(module_config['dimension_step_sizes'], module._dimension_step_sizes)

    def test_parse_module_with_no_parameters(self):
        module_mappings = {"MockModule": TestPipeline.MockProcessorNoParams}
        module_config = {"name": "MockModule"}
        module = Pipeline._parse_module(module_config, module_mappings)
        self.assertEqual(type(module), TestPipeline.MockProcessorNoParams)

    def test_parse_module_with_too_many_parameters(self):
        module_mappings = {"MockModule": TestPipeline.MockProcessorNoParams}
        module_config = {
            "name": "MockModule",
            "bogus_param": True
        }
        self.assertRaises(PipelineBuildingError, Pipeline._parse_module, module_config, module_mappings)

    def test_parse_module_with_missing_parameters(self):
        module_mappings = {"MockModule": TestPipeline.MockProcessorWithParams}
        module_config = {
            "name": "MockModule"
        }

        self.assertRaises(PipelineBuildingError, Pipeline._parse_module, module_config, module_mappings)

    def test_process_tile(self):
        # class MockIdProcessor:
        #     def process(self, tile, *args, **kwargs):
        #         tile.summary.tile_id = "test_id"
        #         return tile
        #
        # class MockReadingProcessor:
        #     def process(self, tile, *args, **kwargs):
        #         dataset = kwargs['dataset']
        #         tile.tile.grid_tile.variable_data.CopyFrom(to_shaped_array(dataset['test_variable']))
        #         return tile
        #
        # test_dataset = xr.Dataset({"test_variable": [1, 2, 3]})
        # input_tile = nexusproto.NexusTile.SerializeToString(NexusTile())
        # processor_list = [MockIdProcessor(), MockReadingProcessor()]
        #
        # output_tile = _process_tile_in_worker(processor_list, test_dataset, input_tile)
        # output_tile = nexusproto.NexusTile.FromString(output_tile)
        # tile_data = from_shaped_array(output_tile.tile.grid_tile.variable_data)
        #
        # np.testing.assert_equal(tile_data, [1, 2, 3])
        # self.assertEqual(output_tile.summary.tile_id, "test_id")
        ...
