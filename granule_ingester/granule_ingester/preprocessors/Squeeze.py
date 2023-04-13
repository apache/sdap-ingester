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

import xarray as xr
from granule_ingester.preprocessors.GranulePreprocessor import GranulePreprocessor

logger = logging.getLogger(__name__)


class Squeeze(GranulePreprocessor):
    def __init__(self, dimensions: list):
        self._dimensions = dimensions

    def process(self, input_dataset: xr.Dataset, *args, **kwargs):
        logger.debug(f'Squeezing dimensions {self._dimensions}')
        output_ds = input_dataset.squeeze(self._dimensions)
        logger.debug(f'Squeezed dimensions: {input_dataset.dims} -> {output_ds.dims}')
        return output_ds
