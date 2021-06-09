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

from nexusproto.serialization import from_shaped_array, to_shaped_array
from nexusproto.DataTile_pb2 import NexusTile
from granule_ingester.processors.TileProcessor import TileProcessor

logger = logging.getLogger(__name__)


class KelvinToCelsius(TileProcessor):
    def __retrieve_var_units(self, variable_name, ds):
        variable_unit = []
        for each in variable_name:
            if 'units' in ds.variables[each].attrs:
                variable_unit.extend(ds.variables[each].attrs['units'])
            elif 'Units' in ds.variables[each].attrs:
                variable_unit.extend(ds.variables[each].attrs['Units'])
            elif 'UNITS' in ds.variables[each].attrs:
                variable_unit.extend(ds.variables[each].attrs['UNITS'])
        return variable_unit

    def process(self, tile: NexusTile, *args, **kwargs):
        logger.debug(f'KelvinToCelsius kwargs: {kwargs}')
        the_tile_type = tile.tile.WhichOneof("tile_type")
        the_tile_data = getattr(tile.tile, the_tile_type)
        logger.debug(the_tile_data.variable_data)
        kelvins = ['kelvin', 'degk', 'deg_k', 'degreesk', 'degrees_k', 'degree_k', 'degreek']

        if 'dataset' in kwargs:
            ds = kwargs['dataset']
            variable_name = tile.summary.data_var_name
            logger.debug(f'K2C tile.summary.data_var_name: {variable_name}')
            logger.debug(f'ds.variables: {ds.variables}')
            variable_unit = self.__retrieve_var_units(variable_name, ds)
            if len(variable_unit) < 1:
                return tile
            variable_unit = [k.lower() for k in variable_unit]
            if any([unit in variable_unit for unit in kelvins]):
                var_data = from_shaped_array(the_tile_data.variable_data) - 273.15
                the_tile_data.variable_data.CopyFrom(to_shaped_array(var_data))
        return tile
