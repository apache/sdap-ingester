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


import logging
import time
from typing import List

import aiomultiprocess
import xarray as xr
import yaml
from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.granule_loaders import GranuleLoader
from granule_ingester.pipeline.Modules import modules as processor_module_mappings
from granule_ingester.processors.TileProcessor import TileProcessor
from granule_ingester.slicers import TileSlicer
from granule_ingester.writers import DataStore, MetadataStore

logger = logging.getLogger(__name__)

MAX_QUEUE_SIZE = 2 ** 15 - 1

_worker_data_store: DataStore = None
_worker_metadata_store: MetadataStore = None
_worker_processor_list: List[TileProcessor] = None
_worker_dataset = None


def _init_worker(processor_list, dataset, data_store_factory, metadata_store_factory):
    global _worker_data_store
    global _worker_metadata_store
    global _worker_processor_list
    global _worker_dataset

    # _worker_data_store and _worker_metadata_store open multiple TCP sockets from each worker process;
    # however, these sockets will be automatically closed by the OS once the worker processes die so no need to worry.
    _worker_data_store = data_store_factory()
    _worker_metadata_store = metadata_store_factory()
    _worker_processor_list = processor_list
    _worker_dataset = dataset


async def _process_tile_in_worker(serialized_input_tile: str):
    global _worker_data_store
    global _worker_metadata_store
    global _worker_processor_list
    global _worker_dataset

    input_tile = nexusproto.NexusTile.FromString(serialized_input_tile)
    processed_tile = _recurse(_worker_processor_list, _worker_dataset, input_tile)
    if processed_tile:
        await _worker_data_store.save_data(processed_tile)
        await _worker_metadata_store.save_metadata(processed_tile)


def _recurse(processor_list: List[TileProcessor],
             dataset: xr.Dataset,
             input_tile: nexusproto.NexusTile) -> nexusproto.NexusTile:
    if len(processor_list) == 0:
        return input_tile
    output_tile = processor_list[0].process(tile=input_tile, dataset=dataset)
    return _recurse(processor_list[1:], dataset, output_tile) if output_tile else None


class Pipeline:
    def __init__(self,
                 granule_loader: GranuleLoader,
                 slicer: TileSlicer,
                 data_store_factory,
                 metadata_store_factory,
                 tile_processors: List[TileProcessor]):
        self._granule_loader = granule_loader
        self._tile_processors = tile_processors
        self._slicer = slicer
        self._data_store_factory = data_store_factory
        self._metadata_store_factory = metadata_store_factory

    @classmethod
    def from_string(cls, config_str: str, data_store_factory, metadata_store_factory):
        config = yaml.load(config_str, yaml.FullLoader)
        return cls._build_pipeline(config,
                                   data_store_factory,
                                   metadata_store_factory,
                                   processor_module_mappings)

    @classmethod
    def from_file(cls, config_path: str, data_store_factory, metadata_store_factory):
        with open(config_path) as config_file:
            config = yaml.load(config_file, yaml.FullLoader)
            return cls._build_pipeline(config,
                                       data_store_factory,
                                       metadata_store_factory,
                                       processor_module_mappings)

    @classmethod
    def _build_pipeline(cls,
                        config: dict,
                        data_store_factory,
                        metadata_store_factory,
                        module_mappings: dict):
        granule_loader = GranuleLoader(**config['granule'])

        slicer_config = config['slicer']
        slicer = cls._parse_module(slicer_config, module_mappings)

        tile_processors = []
        for processor_config in config['processors']:
            module = cls._parse_module(processor_config, module_mappings)
            tile_processors.append(module)

        return cls(granule_loader, slicer, data_store_factory, metadata_store_factory, tile_processors)

    @classmethod
    def _parse_module(cls, module_config: dict, module_mappings: dict):
        module_name = module_config.pop('name')
        try:
            module_class = module_mappings[module_name]
            logger.debug("Loaded processor {}.".format(module_class))
            processor_module = module_class(**module_config)
        except KeyError:
            raise RuntimeError("'{}' is not a valid processor.".format(module_name))

        return processor_module

    async def run(self):
        async with self._granule_loader as (dataset, granule_name):
            start = time.perf_counter()
            async with aiomultiprocess.Pool(initializer=_init_worker,
                                            initargs=(self._tile_processors,
                                                      dataset,
                                                      self._data_store_factory,
                                                      self._metadata_store_factory)) as pool:
                serialized_tiles = [nexusproto.NexusTile.SerializeToString(tile) for tile in
                                    self._slicer.generate_tiles(dataset, granule_name)]
                # aiomultiprocess is built on top of the stdlib multiprocessing library, which has the limitation that
                # a queue can't have more than 2**15-1 tasks. So, we have to batch it.
                for chunk in type(self)._chunk_list(serialized_tiles, MAX_QUEUE_SIZE):
                    await pool.map(_process_tile_in_worker, chunk)

        end = time.perf_counter()
        logger.info("Pipeline finished in {} seconds".format(end - start))

    @staticmethod
    def _chunk_list(items, chunk_size):
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
