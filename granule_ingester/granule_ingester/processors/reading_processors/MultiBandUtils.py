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

logger = logging.getLogger(__name__)
band = 'band'


class MultiBandUtils:
    BAND = 'band'

    @staticmethod
    def move_band_dimension(single_data_subset_dims):
        updated_dims = single_data_subset_dims + [MultiBandUtils.BAND]
        logger.debug(f'updated_dims: {updated_dims}')
        return updated_dims, tuple([k for k in range(1, len(updated_dims))] + [0])
        return tuple(new_dimensions)