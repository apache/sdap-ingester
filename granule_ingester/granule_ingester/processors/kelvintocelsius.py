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
import json
import logging
from copy import deepcopy

from nexusproto.serialization import from_shaped_array, to_shaped_array
from nexusproto.DataTile_pb2 import NexusTile
from granule_ingester.processors.TileProcessor import TileProcessor
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")

logger = logging.getLogger(__name__)


class KelvinToCelsius(TileProcessor):
    def __retrieve_var_units(self, variable_name, ds):
        variable_unit = []
        copied_variable_name = deepcopy(variable_name)
        if not isinstance(copied_variable_name, list):
            copied_variable_name = [copied_variable_name]
        for each in copied_variable_name:
            try:
                logger.debug(f'for ds.variables[each].attrs : {ds.variables[each].attrs}')
                for unit_attr in ('units', 'Units', 'UNITS'):
                    if unit_attr in ds.variables[each].attrs:
                        if isinstance(ds.variables[each].attrs[unit_attr], list):
                            variable_unit.extend(ds.variables[each].attrs[unit_attr])
                        else:
                            variable_unit.append(ds.variables[each].attrs[unit_attr])
                        break
            except Exception as e:
                logger.exception(f'some error in __retrieve_var_units: {str(e)}')
        return variable_unit

    def process(self, tile: NexusTile, *args, **kwargs):
        the_tile_type = tile.tile.WhichOneof("tile_type")
        logger.debug(f'processing granule: {tile.summary.granule}')
        the_tile_data = getattr(tile.tile, the_tile_type)
        kelvins = ['kelvin', 'degk', 'deg_k', 'degreesk', 'degrees_k', 'degree_k', 'degreek']

        if 'dataset' in kwargs:
            ds = kwargs['dataset']
            variable_name = json.loads(tile.summary.data_var_name)
            if not isinstance(variable_name, list):
                variable_name = [variable_name]
            logger.debug(f'K2C tile.summary.data_var_name: {variable_name}')
            variable_unit = self.__retrieve_var_units(variable_name, ds)
            if len(variable_unit) < 1:
                return tile
            variable_unit = [k.lower() for k in variable_unit]
            if any([unit in variable_unit for unit in kelvins]):
                var_data = from_shaped_array(the_tile_data.variable_data) - 273.15
                the_tile_data.variable_data.CopyFrom(to_shaped_array(var_data))
        
        return tile
