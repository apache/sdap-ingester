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
from typing import Dict, Union

import numpy as np
import xarray as xr


from granule_ingester.processors.ZarrProcessor import ZarrProcessor

logger = logging.getLogger(__name__)


class ZarrReadingProcessor(ZarrProcessor, ABC):

    def __init__(self, variable: Union[str, list], latitude: str, longitude: str, *args, **kwargs):
        try:
            # TODO variable in test cases are being passed in as just lists, and is not passable through json.loads() 
            self.variable = json.loads(variable)
        except Exception as e:
            logger.exception(f'failed to convert literal list to python list. using as a single variable: {variable}')
            self.variable = variable
        if isinstance(self.variable, list) and len(self.variable) < 1:
            logger.error(f'variable list is empty: {ZarrReadingProcessor}')
            raise RuntimeError(f'variable list is empty: {self.variable}')
        self.latitude = latitude
        self.longitude = longitude

    @abstractmethod
    def process(self, tile, dataset: xr.Dataset, *args, **kwargs):
        pass
    
    @staticmethod
    def _convert_to_timestamp(times: xr.DataArray) -> xr.DataArray:
        if times.dtype == np.float32:
            return times
        epoch = np.datetime64(datetime.datetime(1970, 1, 1, 0, 0, 0))
        return ((times - epoch) / 1e9).astype(int)
