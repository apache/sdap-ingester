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
        ...

    def detect_grid_type(self, lat: str, lon: str, time: str, processor_types: List[TileReadingProcessor]):
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

        lat = self._detect_dimension_name(lat_regex)
        lon = self._detect_dimension_name(lon_regex)
        time = self._detect_dimension_name(time_regex)

        return (lat, lon, time)

    def _detect_dimension_name(self, pattern: str) -> str:
        candidates = []
        for dim_name in self._dataset.data_vars:
            long_name = self._dataset[dim_name].long_name
            if re.match(pattern, long_name):
                candidates.append(dim_name)
        if len(candidates) > 1:
            raise Exception(f"Found multiple possibilities for dimension with pattern {pattern}.")

        if len(candidates) == 0:
            return None
        return candidates[0]
