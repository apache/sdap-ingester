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

import datetime
from abc import ABC, abstractmethod
from typing import Dict

import numpy as np
import xarray as xr
from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.exceptions import TileProcessingError
from granule_ingester.processors.TileProcessor import TileProcessor


class TileReadingProcessor(TileProcessor, ABC):

    def __init__(self, variable: str, latitude: str, longitude: str, *args, **kwargs):
        self.variable = variable
        self.latitude = latitude
        self.longitude = longitude

    def process(self, tile, dataset: xr.Dataset, *args, **kwargs):
        try:
            dimensions_to_slices = self._convert_spec_to_slices(tile.summary.section_spec)

            output_tile = nexusproto.NexusTile()
            output_tile.CopyFrom(tile)
            output_tile.summary.data_var_name = self.variable

            return self._generate_tile(dataset, dimensions_to_slices, output_tile)
        except Exception:
            raise TileProcessingError("Could not generate tiles from the granule.")

    @abstractmethod
    def _generate_tile(self, dataset: xr.Dataset, dimensions_to_slices: Dict[str, slice], tile):
        pass

    @classmethod
    def _parse_input(cls, the_input_tile, temp_dir):
        specs = the_input_tile.summary.section_spec
        tile_specifications = cls._convert_spec_to_slices(specs)

        file_path = the_input_tile.summary.granule
        file_path = file_path[len('file:'):] if file_path.startswith('file:') else file_path

        return tile_specifications, file_path

    @staticmethod
    def _slices_for_variable(variable: xr.DataArray, dimension_to_slice: Dict[str, slice]) -> Dict[str, slice]:
        return {dim_name: dimension_to_slice[dim_name] for dim_name in variable.dims}

    @staticmethod
    def _convert_spec_to_slices(spec):
        dim_to_slice = {}
        for dimension in spec.split(','):
            name, start, stop = dimension.split(':')
            dim_to_slice[name] = slice(int(start), int(stop))

        return dim_to_slice

    @staticmethod
    def _convert_to_timestamp(times: xr.DataArray) -> xr.DataArray:
        if times.dtype == np.float32:
            return times
        epoch = np.datetime64(datetime.datetime(1970, 1, 1, 0, 0, 0))
        return ((times - epoch) / 1e9).astype(int)
