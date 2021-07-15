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

import numpy
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array

from granule_ingester.processors.TileProcessor import TileProcessor

logger = logging.getLogger(__name__)


def parse_input(nexus_tile_data):
    return nexusproto.NexusTile.FromString(nexus_tile_data)


class EmptyTileFilter(TileProcessor):
    def process(self, tile, *args, **kwargs):
        logger.debug(f'processing: {tile}')
        tile_type = tile.tile.WhichOneof("tile_type")
        tile_data = getattr(tile.tile, tile_type)
        data = from_shaped_array(tile_data.variable_data)
        # Only supply data if there is actual values in the tile
        if data.size - numpy.count_nonzero(numpy.isnan(data)) > 0:
            return tile
        else:
            logger.warning("Discarding tile from {} because it is empty".format(tile.summary.granule))
        return None
