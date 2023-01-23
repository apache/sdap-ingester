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
# from typing import List, Dict, Tuple,Set
#
# # from granule_ingester.entities import Dimension
#
#
# class SliceFileByDimension:
#     def __init__(self,
#                  slice_dimension: str,    # slice by this dimension
#                  dimension_name_prefix: str = None,
#                  *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._slice_by_dimension = slice_dimension
#         self._dimension_name_prefix = dimension_name_prefix
#
#     def generate_slices(self, dimension_specs: Dict[str, int]) -> List[str]:
#         """
#         Generate list of slices in all dimensions as strings.
#
#         :param dimension_specs: A dict of dimension names to dimension lengths
#         :return: A list of slices across all dimensions, as strings in the form of dim1:0:720,dim2:0:1,dim3:0:360
#         """
#         # check if sliceDimension is int or str
#         is_integer: bool = False
#         try:
#             int(self._slice_by_dimension)
#             is_integer = True
#         except ValueError:
#             pass
#
#         return self._indexed_dimension_slicing(dimension_specs) if is_integer else self._generate_tile_boundary_slices(self._slice_by_dimension,dimension_specs)
#
#     def _indexed_dimension_slicing(self, dimension_specs):
#         # python netCDF4 library automatically prepends "phony_dim" if indexed by integer
#         if self._dimension_name_prefix == None or self._dimension_name_prefix == "":
#             self._dimension_name_prefix = "phony_dim_"
#
#         return self._generate_tile_boundary_slices((self._dimension_name_prefix+self._slice_by_dimension),dimension_specs)
#
#     def _generate_tile_boundary_slices(self, slice_by_dimension, dimension_specs):
#         dimension_bounds = []
#         for dim_name,dim_len in dimension_specs.items():
#             step_size = 1 if dim_name==slice_by_dimension else dim_len
#
#             bounds = []
#             for i in range(0,dim_len,step_size):
#                 bounds.append(
#                     '{name}:{start}:{end}'.format(name=dim_name,
#                                     start=i,
#                                     end=min((i + step_size),dim_len) )
#                 )
#             dimension_bounds.append(bounds)
#         return [','.join(chunks) for chunks in itertools.product(*dimension_bounds)]
#
