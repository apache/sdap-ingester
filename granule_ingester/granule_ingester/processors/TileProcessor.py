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
from nexusproto.serialization import from_shaped_array, to_shaped_array
from nexusproto.DataTile_pb2 import NexusTile
from granule_ingester.processors.TileProcessor import TileProcessor


# TODO: make this an informal interface, not an abstract class
class TileProcessor(ABC):
    @abstractmethod
    def process(self, tile: NexusTile, *args, **kwargs):
        ## accessing the data
        # the_tile_type = tile.tile.WhichOneof("tile_type")
        # the_tile_data = getattr(tile.tile, the_tile_type)

        ## get netCDF as xarray.Dataset object        
        # ds = kwargs['dataset']

        ## example transformation:
        # var_data = from_shaped_array(the_tile_data.variable_data) - 273.15

        ## save transformed data back into tile
        # the_tile_data.variable_data.CopyFrom(to_shaped_array(var_data))

        ## return transformed tile
        # return tile

        pass
