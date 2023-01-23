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

# import math
# import itertools
# from typing import List, Dict, Tuple
#
# # from granule_ingester.entities import Dimension
#
#
# class SliceFileByTilesDesired:
#     def __init__(self,
#                  tiles_desired: int,
#                  desired_spatial_dimensions: List[str],
#                  time_dimension: Tuple[str, int] = None,
#                  *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._tiles_desired = tiles_desired
#
#         # check that desired_dimensions have no duplicates
#         self._desired_spatial_dimensions = desired_spatial_dimensions
#         self._time_dimension = time_dimension
#
#     def generate_slices(self,
#                         dimension_specs: Dict[str, int]) -> List[str]:
#         # check that dimension_specs contain all desired_dimensions
#         # check that there are no duplicate keys in dimension_specs
#         desired_dimension_specs = {key: dimension_specs[key]
#                                    for key in self._desired_spatial_dimensions}
#         spatial_slices = self._generate_spatial_slices(tiles_desired=self._tiles_desired,
#                                                        dimension_specs=desired_dimension_specs)
#         if self._time_dimension:
#             temporal_slices = self._generate_temporal_slices(self._time_dimension)
#             return self._add_temporal_slices(temporal_slices, spatial_slices)
#         return spatial_slices
#
#     def _add_temporal_slices(self, temporal_slices, spatial_slices) -> List[str]:
#         return ['{time},{space}'.format(time=temporal_slice, space=spatial_slice)
#                 for spatial_slice in spatial_slices
#                 for temporal_slice in temporal_slices]
#
#     def _generate_temporal_slices(self, time_dimension: Tuple[str, int]):
#         return ['{time_dim}:{start}:{end}'.format(time_dim=time_dimension[0],
#                                                   start=i,
#                                                   end=i+1)
#                 for i in range(time_dimension[1]-1)]
#
#     def _generate_spatial_slices(self,
#                                  tiles_desired: int,
#                                  dimension_specs: Dict[str, int]) -> List[str]:
#         n_dimensions = len(dimension_specs)
#         dimension_bounds = []
#         for dim_name, dim_length in dimension_specs.items():
#             step_size = SliceFileByTilesDesired._calculate_step_size(
#                 dim_length, tiles_desired, n_dimensions)
#             bounds = []
#             start_loc = 0
#             while start_loc < dim_length:
#                 end_loc = start_loc + step_size if start_loc + \
#                     step_size < dim_length else dim_length
#                 bounds.append('{name}:{start}:{end}'.format(name=dim_name,
#                                                             start=start_loc,
#                                                             end=end_loc))
#                 start_loc += step_size
#             dimension_bounds.append(bounds)
#         return [','.join(chunks) for chunks in itertools.product(*dimension_bounds)]
#
#     @staticmethod
#     def _calculate_step_size(dim_length, chunks_desired, n_dimensions) -> int:
#         chunks_per_dim = math.pow(chunks_desired, 1.0 / n_dimensions)
#         return math.floor(dim_length / chunks_per_dim)
