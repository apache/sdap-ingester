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
import pickle
import time
from multiprocessing import Manager
from typing import List

import xarray as xr
import yaml

from aiomultiprocess import Pool
from aiomultiprocess.types import ProxyException
from granule_ingester.exceptions import PipelineBuildingError
from granule_ingester.granule_loaders import GranuleLoader
from granule_ingester.pipeline.Modules import \
    modules as processor_module_mappings
from granule_ingester.processors.TileProcessor import TileProcessor
from granule_ingester.slicers import TileSlicer
from granule_ingester.writers import DataStore, MetadataStore
from nexusproto import DataTile_pb2 as nexusproto
from tblib import pickling_support

logger = logging.getLogger(__name__)

# The aiomultiprocessing library has a bug where it never closes out the pool if there are more than a certain
# number of items to process. The exact number is unknown, but 2**8-1 is safe.
MAX_CHUNK_SIZE = 2 ** 8 - 1

_worker_data_store: DataStore = None
_worker_metadata_store: MetadataStore = None
_worker_processor_list: List[TileProcessor] = None
_worker_dataset = None
_shared_memory = None


def _init_worker(processor_list, dataset, data_store_factory, metadata_store_factory, shared_memory):
    global _worker_data_store
    global _worker_metadata_store
    global _worker_processor_list
    global _worker_dataset
    global _shared_memory

    # _worker_data_store and _worker_metadata_store open multiple TCP sockets from each worker process;
    # however, these sockets will be automatically closed by the OS once the worker processes die so no need to worry.
    _worker_data_store = data_store_factory()
    _worker_metadata_store = metadata_store_factory()
    _worker_processor_list = processor_list
    _worker_dataset = dataset
    _shared_memory = shared_memory

    logger.debug("worker init")


def _init_write_worker(data_store_factory, metadata_store_factory):
    global _worker_metadata_store
    global _worker_processor_list

    _worker_data_store = data_store_factory()
    _worker_metadata_store = metadata_store_factory()


async def _write_tile_batch(tiles: str):
    print(type(tiles[0]), flush=True)

    print(tiles)

    deserialized_tiles = [nexusproto.NexusTile.FromString(t) for t in tiles]

    # await _worker_data_store.save_batch(deserialized_tiles)
    await _worker_metadata_store.save_batch(deserialized_tiles)


async def _process_tile_in_worker(serialized_input_tile: str):
    try:
        logger.debug(f'serialized_input_tile: {serialized_input_tile}')
        input_tile = nexusproto.NexusTile.FromString(serialized_input_tile)
        logger.debug(f'_recurse params: _worker_processor_list = {_worker_processor_list}, _worker_dataset = {_worker_dataset}, input_tile = {input_tile}')
        processed_tile: nexusproto = _recurse(_worker_processor_list, _worker_dataset, input_tile)

        if processed_tile:
            # await _worker_data_store.save_data(processed_tile)
            # await _worker_metadata_store.save_metadata(processed_tile)

            return nexusproto.NexusTile.SerializeToString(processed_tile)
        else:
            return None
    except Exception as e:
        pickling_support.install(e)
        _shared_memory.error = pickle.dumps(e)
        raise


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
                 tile_processors: List[TileProcessor],
                 max_concurrency: int):
        self._granule_loader = granule_loader
        self._tile_processors = tile_processors
        self._slicer = slicer
        self._data_store_factory = data_store_factory
        self._metadata_store_factory = metadata_store_factory
        self._max_concurrency = max_concurrency

        # Create a SyncManager so that we can to communicate exceptions from the
        # worker processes back to the main process.
        self._manager = Manager()

    def __del__(self):
        self._manager.shutdown()

    @classmethod
    def from_string(cls, config_str: str, data_store_factory, metadata_store_factory, max_concurrency: int = 16):
        logger.debug(f'config_str: {config_str}')
        try:
            config = yaml.load(config_str, yaml.FullLoader)
            cls._validate_config(config)
            return cls._build_pipeline(config,
                                       data_store_factory,
                                       metadata_store_factory,
                                       processor_module_mappings,
                                       max_concurrency)

        except yaml.scanner.ScannerError:
            raise PipelineBuildingError("Cannot build pipeline because of a syntax error in the YAML.")

    # TODO: this method should validate the config against an actual schema definition
    @staticmethod
    def _validate_config(config: dict):
        if type(config) is not dict:
            raise PipelineBuildingError("Cannot build pipeline; the pipeline configuration that " +
                                        "was received is not valid YAML.")

    @classmethod
    def _build_pipeline(cls,
                        config: dict,
                        data_store_factory,
                        metadata_store_factory,
                        module_mappings: dict,
                        max_concurrency: int):
        try:
            granule_loader = GranuleLoader(**config['granule'])

            slicer_config = config['slicer']
            slicer = cls._parse_module(slicer_config, module_mappings)

            tile_processors = []
            for processor_config in config['processors']:
                module = cls._parse_module(processor_config, module_mappings)
                tile_processors.append(module)

            return cls(granule_loader,
                       slicer,
                       data_store_factory,
                       metadata_store_factory,
                       tile_processors,
                       max_concurrency)
        except PipelineBuildingError:
            raise
        except KeyError as e:
            raise PipelineBuildingError(f"Cannot build pipeline because {e} is missing from the YAML.")
        except Exception as e:
            logger.exception(e)
            raise PipelineBuildingError(f"Cannot build pipeline because of the following error: {e}")

    @classmethod
    def _parse_module(cls, module_config: dict, module_mappings: dict):
        module_name = module_config.pop('name')
        try:
            module_class = module_mappings[module_name]
            logger.debug("Loaded processor {}.".format(module_class))
            processor_module = module_class(**module_config)
        except KeyError:
            raise PipelineBuildingError(f"'{module_name}' is not a valid processor.")
        except Exception as e:
            raise PipelineBuildingError(f"Parsing module '{module_name}' failed because of the following error: {e}")

        return processor_module

    async def run(self):
        async with self._granule_loader as (dataset, granule_name):
            start = time.perf_counter()

            shared_memory = self._manager.Namespace()
            async with Pool(initializer=_init_worker,
                            initargs=(self._tile_processors,
                                      dataset,
                                      self._data_store_factory,
                                      self._metadata_store_factory,
                                      shared_memory),
                            maxtasksperchild=self._max_concurrency,
                            childconcurrency=self._max_concurrency) as pool:
                serialized_tiles = [nexusproto.NexusTile.SerializeToString(tile) for tile in
                                    self._slicer.generate_tiles(dataset, granule_name)]
                # aiomultiprocess is built on top of the stdlib multiprocessing library, which has the limitation that
                # a queue can't have more than 2**15-1 tasks. So, we have to batch it.

                tile_gen_end = time.perf_counter()

                logger.info(f"Finished generating tiles in {start - tile_gen_end}")

                results = []

                for chunk in self._chunk_list(serialized_tiles, MAX_CHUNK_SIZE):
                    try:
                        for r in await pool.map(_process_tile_in_worker, chunk):
                            if r is not None:
                                results.append(nexusproto.NexusTile.FromString(r))

                    except ProxyException:
                        pool.terminate()
                        # Give the shared memory manager some time to write the exception
                        # await asyncio.sleep(1)
                        raise pickle.loads(shared_memory.error)

                await self._metadata_store_factory().save_batch(results)
                await self._data_store_factory().save_batch(results)


        end = time.perf_counter()
        logger.info("Pipeline finished in {} seconds".format(end - start))

    @staticmethod
    def _chunk_list(items, chunk_size: int):
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
