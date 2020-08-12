from typing import Dict

import numpy as np
import xarray as xr
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import to_shaped_array

from granule_ingester.processors.reading_processors.TileReadingProcessor import TileReadingProcessor


class EccoReadingProcessor(TileReadingProcessor):
    def __init__(self,
                 variable_to_read,
                 latitude,
                 longitude,
                 tile,
                 depth=None,
                 time=None,
                 **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        self.depth = depth
        self.time = time
        self.tile = tile

    @staticmethod
    def bid(dataset, variable, lat, lon, time):
        bid = 0
        if lat == 'YC' and lon == 'XC':
            bid += 1
        if lat not in dataset[variable].dims and lon not in dataset[variable].dims:
            bid += 1
        if 'tile' in dataset[variable].dims:
            bid += 1

        return bid / 3

    def _generate_tile(self, ds: xr.Dataset, dimensions_to_slices: Dict[str, slice], input_tile):
        new_tile = nexusproto.EccoTile()

        lat_subset = ds[self.latitude][type(self)._slices_for_variable(ds[self.latitude], dimensions_to_slices)]
        lon_subset = ds[self.longitude][type(self)._slices_for_variable(ds[self.longitude], dimensions_to_slices)]
        lat_subset = np.ma.filled(np.squeeze(lat_subset), np.NaN)
        lon_subset = np.ma.filled(np.squeeze(lon_subset), np.NaN)

        data_subset = ds[self.variable_to_read][
            type(self)._slices_for_variable(ds[self.variable_to_read], dimensions_to_slices)]
        data_subset = np.ma.filled(np.squeeze(data_subset), np.NaN)

        new_tile.tile = ds[self.tile][dimensions_to_slices[self.tile].start].item()

        if self.depth:
            depth_dim, depth_slice = list(type(self)._slices_for_variable(ds[self.depth],
                                                                          dimensions_to_slices).items())[0]
            depth_slice_len = depth_slice.stop - depth_slice.start
            if depth_slice_len > 1:
                raise RuntimeError(
                    "Depth slices must have length 1, but '{dim}' has length {dim_len}.".format(dim=depth_dim,
                                                                                                dim_len=depth_slice_len))
            new_tile.depth = ds[self.depth][depth_slice].item()

        if self.time:
            time_slice = dimensions_to_slices[self.time]
            time_slice_len = time_slice.stop - time_slice.start
            if time_slice_len > 1:
                raise RuntimeError(
                    "Time slices must have length 1, but '{dim}' has length {dim_len}.".format(dim=self.time,
                                                                                               dim_len=time_slice_len))
            new_tile.time = int(ds[self.time][time_slice.start].item() / 1e9)

        new_tile.latitude.CopyFrom(to_shaped_array(lat_subset))
        new_tile.longitude.CopyFrom(to_shaped_array(lon_subset))
        new_tile.variable_data.CopyFrom(to_shaped_array(data_subset))

        input_tile.tile.ecco_tile.CopyFrom(new_tile)
        return input_tile
