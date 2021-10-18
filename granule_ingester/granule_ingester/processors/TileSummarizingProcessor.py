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
import os
import json
import logging
import re
import datetime

import numpy
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import from_shaped_array

from granule_ingester.processors.TileProcessor import TileProcessor
logger = logging.getLogger(__name__)


class NoTimeException(Exception):
    pass


def find_time_min_max(tile_data, time_from_granule=None):
    if tile_data.time:
        if isinstance(tile_data.time, nexusproto.ShapedArray):
            time_data = from_shaped_array(tile_data.time)
            return int(numpy.nanmin(time_data).item()), int(numpy.nanmax(time_data).item())
        elif isinstance(tile_data.time, int) and \
             tile_data.time > datetime.datetime(1970, 1, 1).timestamp():  # time should be at least greater than Epoch, right?
            return tile_data.time, tile_data.time

    if time_from_granule:
        return time_from_granule, time_from_granule

    raise NoTimeException


def get_time_from_granule(granule: str) -> int:
    """
    Get time from granule name. It makes the assumption that a datetime is
    specificed in the granule name, and it has the following format "YYYYddd",
    where YYYY is 4 digit year and ddd is day of year (i.e. 1 to 365).
    
    Note: This is a very narrow implmentation for a specific need. If you found
    yourself needing to modify this function to accommodate more use cases, then
    perhaps it is time to refactor this function to a more dynamic module.
    """

    # rs search for a sub str which starts with 19 or 20, and has 7 digits
    search_res = re.search('((?:19|20)[0-9]{2})([0-9]{3})', os.path.basename(granule))
    if not search_res:
        return None
    
    year, days = search_res.groups()
    year = int(year)
    days = int(days)

    # since datetime is set to 1/1 (+1 day), timedelta needs to -1 to cancel it out
    return int((datetime.datetime(year, 1, 1) + datetime.timedelta(days-1)).timestamp())


class TileSummarizingProcessor(TileProcessor):

    def __init__(self, dataset_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataset_name = dataset_name

    def process(self, tile, dataset, *args, **kwargs):
        tile_type = tile.tile.WhichOneof("tile_type")
        logger.debug(f'processing granule: {tile.summary.granule}')
        tile_data = getattr(tile.tile, tile_type)

        latitudes = numpy.ma.masked_invalid(from_shaped_array(tile_data.latitude))
        longitudes = numpy.ma.masked_invalid(from_shaped_array(tile_data.longitude))
        data = from_shaped_array(tile_data.variable_data)
        logger.debug(f'retrieved lat, long, data')

        tile_summary = tile.summary if tile.HasField("summary") else nexusproto.TileSummary()
        logger.debug(f'retrieved summary')

        tile_summary.dataset_name = self._dataset_name
        tile_summary.bbox.lat_min = numpy.nanmin(latitudes).item()
        tile_summary.bbox.lat_max = numpy.nanmax(latitudes).item()
        tile_summary.bbox.lon_min = numpy.nanmin(longitudes).item()
        tile_summary.bbox.lon_max = numpy.nanmax(longitudes).item()
        tile_summary.stats.min = numpy.nanmin(data).item()
        tile_summary.stats.max = numpy.nanmax(data).item()
        tile_summary.stats.count = data.size - numpy.count_nonzero(numpy.isnan(data))
        logger.debug(f'set summary fields')

        data_var_name = json.loads(tile_summary.data_var_name)
        if not isinstance(data_var_name, list):
            data_var_name = [data_var_name]
        # In order to accurately calculate the average we need to weight the data based on the cosine of its latitude
        # This is handled slightly differently for swath vs. grid data
        if tile_type == 'swath_tile':
            # For Swath tiles, len(data) == len(latitudes) == len(longitudes).
            # So we can simply weight each element in the data array
            tile_summary.stats.mean = type(self).calculate_mean_for_swath_tile(data, latitudes)
        elif tile_type == 'grid_tile':
            # Grid tiles need to repeat the weight for every longitude
            # TODO This assumes data axis' are ordered as latitude x longitude
            logger.debug(f'set grid mean. tile_summary.data_var_name: {tile_summary.data_var_name}')

            try:
                tile_summary.stats.mean = type(self).calculate_mean_for_grid_tile(data, latitudes, longitudes, len(data_var_name))
            except Exception as e:
                logger.exception(f'error while setting grid mean: {str(e)}')
                tile_summary.stats.mean = 0
        else:
            # Default to simple average with no weighting
            tile_summary.stats.mean = numpy.nanmean(data).item()
        logger.debug(f'find min max time')

        try:
            min_time, max_time = find_time_min_max(
                tile_data, get_time_from_granule(tile.summary.granule)
            )
            logger.debug(f'set min max time')
            tile_summary.stats.min_time = min_time
            tile_summary.stats.max_time = max_time
        except NoTimeException:
            pass
        logger.debug(f'calc standard_name')
        standard_names = [dataset.variables[k].attrs.get('standard_name')for k in data_var_name]
        logger.debug(f'using standard_names: {standard_names}')
        tile_summary.standard_name = json.dumps(standard_names)
        logger.debug(f'copy tile_summary to tile')
        tile.summary.CopyFrom(tile_summary)
        return tile

    @staticmethod
    def calculate_mean_for_grid_tile(variable_data, latitudes, longitudes, data_var_name_len=1):
        flattened_variable_data = numpy.ma.masked_invalid(variable_data).flatten()
        repeated_latitudes = numpy.repeat(latitudes, len(longitudes) * data_var_name_len)
        weights = numpy.cos(numpy.radians(repeated_latitudes))
        return numpy.ma.average(flattened_variable_data, weights=weights).item()

    @staticmethod
    def calculate_mean_for_swath_tile(variable_data, latitudes):
        weights = numpy.cos(numpy.radians(latitudes))
        return numpy.ma.average(numpy.ma.masked_invalid(variable_data),
                                weights=weights).item()
