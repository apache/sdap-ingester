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
import json
import logging
from abc import ABC, abstractmethod
from time import time
from typing import Dict, Union, List

import numpy as np
import xarray as xr
from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.exceptions import TileProcessingError
from granule_ingester.processors.TileProcessor import TileProcessor

logger = logging.getLogger(__name__)


class ZarrProcessor():

    def __init__(self, variable: Union[str, list], latitude: str, longitude: str, time=None, *args, **kwargs):
        try:
            # TODO variable in test cases are being passed in as just lists, and is not passable through json.loads()
            self.variable = json.loads(variable)
        except Exception as e:
            logger.exception(f'failed to convert literal list to python list. using as a single variable: {variable}')
            self.variable = variable
        if isinstance(self.variable, list) and len(self.variable) < 1:
            logger.error(f'variable list is empty: {self}')
            raise RuntimeError(f'variable list is empty: {self.variable}')
        self.latitude = latitude
        self.longitude = longitude
        self.time = time
        

    def process(self, granule: xr.Dataset, process_list: List,  *args, **kwargs):
        for processes in process_list['processors']:
            logger.debug(f'Reading Processor: {type(processes)}')
            try:
                # grab ingestion message's processors
                processName = processes['name']
                # TODO process granule via methods in this class
            except Exception as e:
                raise TileProcessingError(f"Could not generate tiles from the granule because of the following error: {e}.")
        # returns granule. Passed into writers, which then push into S3
        return granule
 
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
    
    # TODO passing in granule with single variable or with all? NEEDS TESTING
    def kelvinToCelsius(self, granule: xr.Dataset):
        for dataVar in self.variable:
            logger.debug(f'converting kelvin to celsius for variable {dataVar}')
            data_var = granule[dataVar]
            
            var_celsius = data_var - 273.15
            var_attrs = data_var.attrs
            var_celsius.attrs = var_attrs
            var_celsius.attrs['units'] = 'celsius'
            granule[dataVar] = var_celsius
    
    # TODO needs testing
    def forceAscendingLatitude(self, granule: xr.Dataset):
        granule = granule.sortby('lat', ascending=False)
        
    # TODO below methods needs implementation
    def GenerateTileId(self, granule: xr.Dataset):
        pass
    
    def Subtract180FromLongitude(self, granule: xr.Dataset):
        pass

        
