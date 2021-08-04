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
import os
import uuid

from nexusproto import DataTile_pb2 as nexusproto
from granule_ingester.processors.TileProcessor import TileProcessor
logger = logging.getLogger(__name__)


class GenerateTileId(TileProcessor):

    def process(self, tile: nexusproto.NexusTile, *args, **kwargs):
        logger.debug(f'processing granule: {tile.summary.granule}')
        granule = os.path.basename(tile.summary.granule)
        variable_name = tile.summary.data_var_name
        spec = tile.summary.section_spec
        dataset_name = tile.summary.dataset_name

        generated_id = uuid.uuid3(uuid.NAMESPACE_DNS, dataset_name + granule + str(variable_name) + spec)
        logger.debug(f'generated_id: {generated_id}')
        tile.summary.tile_id = str(generated_id)
        return tile
