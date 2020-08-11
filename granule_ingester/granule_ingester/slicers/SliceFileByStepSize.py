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

import itertools
import logging
from typing import Dict, List

from granule_ingester.exceptions import TileProcessingError
from granule_ingester.slicers.TileSlicer import TileSlicer

logger = logging.getLogger(__name__)


class SliceFileByStepSize(TileSlicer):
    def __init__(self,
                 dimension_step_sizes: Dict[str, int],
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dimension_step_sizes = dimension_step_sizes

    def _generate_slices(self, dimension_specs: Dict[str, int]) -> List[str]:
        # make sure all provided dimensions are in dataset
        for dim_name in self._dimension_step_sizes.keys():
            if dim_name not in list(dimension_specs.keys()):
                raise TileProcessingError('Provided dimension "{}" not found in dataset'.format(dim_name))

        slices = self._generate_chunk_boundary_slices(dimension_specs)
        logger.info("Sliced granule into {} slices.".format(len(slices)))
        return slices

    def _generate_chunk_boundary_slices(self, dimension_specs) -> list:
        dimension_bounds = []
        dim_step_keys = self._dimension_step_sizes.keys()

        for dim_name, dim_len in dimension_specs.items():
            step_size = self._dimension_step_sizes[dim_name] if dim_name in dim_step_keys else dim_len

            bounds = []
            for i in range(0, dim_len, step_size):
                bounds.append('{name}:{start}:{end}'.format(name=dim_name,
                                                            start=i,
                                                            end=min((i + step_size), dim_len)))
            dimension_bounds.append(bounds)
        return [','.join(chunks) for chunks in itertools.product(*dimension_bounds)]
