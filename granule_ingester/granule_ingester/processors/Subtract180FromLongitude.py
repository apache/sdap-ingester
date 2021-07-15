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

from granule_ingester.processors.TileProcessor import TileProcessor
from nexusproto.serialization import from_shaped_array, to_shaped_array

logger = logging.getLogger(__name__)


class Subtract180FromLongitude(TileProcessor):
    def process(self, tile, *args, **kwargs):
        """
        This method will transform longitude values in degrees_east from 0 TO 360 to -180 to 180
        :param self:
        :param nexus_tile: The nexus_tile
        :return: Tile data with altered longitude values
        """
        logger.debug(f'processing: {tile}')
        the_tile_type = tile.tile.WhichOneof("tile_type")
        the_tile_data = getattr(tile.tile, the_tile_type)
        longitudes = from_shaped_array(the_tile_data.longitude)

        # Only subtract 360 if the longitude is greater than 180
        longitudes[longitudes > 180] -= 360

        the_tile_data.longitude.CopyFrom(to_shaped_array(longitudes))

        return tile
