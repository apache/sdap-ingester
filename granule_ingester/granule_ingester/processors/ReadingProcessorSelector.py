import xarray as xr
from typing import List
import re

from granule_ingester.processors.reading_processors import (TileReadingProcessor,
                                                            GridReadingProcessor,
                                                            EccoReadingProcessor,
                                                            SwathReadingProcessor,
                                                            TimeSeriesReadingProcessor)


GRID_PROCESSORS = [GridReadingProcessor, EccoReadingProcessor, SwathReadingProcessor, TimeSeriesReadingProcessor]


class ReadingProcessorSelector:
    def __init__(self, dataset: xr.Dataset, variable: str, *args, **kwargs):
        self._dataset = dataset
        self._variable = variable

    def get_reading_processor(self):
        lat, lon, time = self.detect_dimensions()
        processor_class = self.detect_grid_type(lat=lat, lon=lon, time=time, processor_types=GRID_PROCESSORS)
        return processor_class(variable_to_read=self._variable, latitude=lat, longitude=lon, time=time)

    def detect_grid_type(self,
                         lat: str,
                         lon: str,
                         time: str,
                         processor_types: List[TileReadingProcessor]):
        bids = []
        for processor_type in processor_types:
            bid = processor_type.bid(dataset=self._dataset,
                                     variable=self._variable,
                                     lat=lat,
                                     lon=lon,
                                     time=time)
            bids.append((processor_type, bid))
        highest_bidder = max(bids, key=lambda bidder: bidder[1])

        return highest_bidder[0]

    def detect_dimensions(self):
        lat_regex = r'((.*\s+)?latitude(.*\s+)?)|((.*\s+)?lat(\s+.*)?)'
        lon_regex = r'((.*\s+)?longitude(.*\s+)?)|((.*\s+)?lon(\s+.*)?)'
        time_regex = r'(.*\s+)?time(.*\s+)?'

        dims = self._dataset.data_vars
        lat = self._find_dimension_in_list(lat_regex, dims)
        lon = self._find_dimension_in_list(lon_regex, dims)
        time = self._find_dimension_in_list(time_regex, dims)

        return (lat, lon, time)

    def _find_dimension_in_list(self, pattern: str, dims: List[str], use_long_name=True) -> str:
        candidates = []
        for dim_name in dims:
            if use_long_name:
                name = self._dataset[dim_name].long_name
            else:
                name = dim_name
            if re.match(pattern, name):
                candidates.append(dim_name)
        if len(candidates) > 1:
            raise Exception(f"Found multiple possibilities for dimension with pattern {pattern}.")

        if len(candidates) == 0:
            return None
        return candidates[0]

    def _detect_step_sizes(self, dataset: xr.Dataset, variable_name, slice_time=True):
        dimensions = dataset[variable_name].dims
        time_dim = self._find_dimension_in_list(r'(.*)?time(.*)?', dimensions, use_long_name=False)

        spatial_dims = set(dimensions[-2:]) - {time_dim}
        other_dims = set(dimensions[:-2]) - {time_dim}

        spatial_step_sizes = {dim_name: 30 for dim_name in spatial_dims}
        other_step_sizes = {dim_name: 1 for dim_name in other_dims}
        if time_dim:
            if slice_time:
                time_step_size = {time_dim: 1}
            else:
                time_step_size = {time_dim: dataset[variable_name].sizes[time_dim]}
        else:
            time_step_size = {}

        return {**other_step_sizes, **spatial_step_sizes, **time_step_size}
