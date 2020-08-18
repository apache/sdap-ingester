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

from abc import ABC, abstractmethod
from typing import List

import xarray as xr
from nexusproto.DataTile_pb2 import NexusTile
import re


class TileSlicer(ABC):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._granule_name = None
        self._current_tile_spec_index = 0
        self._tile_spec_list: List[str] = []

    def __iter__(self):
        return self

    def __next__(self) -> NexusTile:
        if self._current_tile_spec_index == len(self._tile_spec_list):
            raise StopIteration

        current_tile_spec = self._tile_spec_list[self._current_tile_spec_index]
        self._current_tile_spec_index += 1

        tile = NexusTile()
        tile.summary.section_spec = current_tile_spec
        tile.summary.granule = self._granule_name
        return tile

    def generate_tiles(self, dataset: xr.Dataset, variable_name: str, granule_name: str = None):
        self._granule_name = granule_name
        dimensions = dataset.dims
        step_sizes = self._detect_step_sizes(dataset, variable_name)
        self._tile_spec_list = self._generate_slices(dimensions, step_sizes)

        return self

    @abstractmethod
    def _generate_slices(self, dimensions):
        pass
