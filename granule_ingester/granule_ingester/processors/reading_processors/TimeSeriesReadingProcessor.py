from typing import Dict

import numpy as np
import xarray as xr
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import to_shaped_array

from granule_ingester.processors.reading_processors.TileReadingProcessor import TileReadingProcessor


class TimeSeriesReadingProcessor(TileReadingProcessor):
    def __init__(self, variable_to_read, latitude, longitude, time, depth=None, **kwargs):
        super().__init__(variable_to_read, latitude, longitude, **kwargs)

        self.depth = depth
        self.time = time

    def _generate_tile(self, ds: xr.Dataset, dimensions_to_slices: Dict[str, slice], input_tile):
        new_tile = nexusproto.TimeSeriesTile()

        lat_subset = ds[self.latitude][type(self)._slices_for_variable(ds[self.latitude], dimensions_to_slices)]
        lon_subset = ds[self.longitude][type(self)._slices_for_variable(ds[self.longitude], dimensions_to_slices)]
        lat_subset = np.ma.filled(lat_subset, np.NaN)
        lon_subset = np.ma.filled(lon_subset, np.NaN)

        data_subset = ds[self.variable_to_read][type(self)._slices_for_variable(ds[self.variable_to_read],
                                                                                dimensions_to_slices)]
        data_subset = np.ma.filled(data_subset, np.NaN)

        if self.depth:
            depth_dim, depth_slice = list(type(self)._slices_for_variable(ds[self.depth],
                                                                          dimensions_to_slices).items())[0]
            depth_slice_len = depth_slice.stop - depth_slice.start
            if depth_slice_len > 1:
                raise RuntimeError(
                    "Depth slices must have length 1, but '{dim}' has length {dim_len}.".format(dim=depth_dim,
                                                                                                dim_len=depth_slice_len))
            new_tile.depth = ds[self.depth][depth_slice].item()

        time_subset = ds[self.time][type(self)._slices_for_variable(ds[self.time], dimensions_to_slices)]
        time_subset = np.ma.filled(type(self)._convert_to_timestamp(time_subset), np.NaN)

        new_tile.latitude.CopyFrom(to_shaped_array(lat_subset))
        new_tile.longitude.CopyFrom(to_shaped_array(lon_subset))
        new_tile.variable_data.CopyFrom(to_shaped_array(data_subset))
        new_tile.time.CopyFrom(to_shaped_array(time_subset))

        input_tile.tile.time_series_tile.CopyFrom(new_tile)
        return input_tile

    # def read_data(self, tile_specifications, file_path, output_tile):
    #     with xr.decode_cf(xr.open_dataset(file_path, decode_cf=False), decode_times=False) as ds:
    #         for section_spec, dimtoslice in tile_specifications:
    #             tile = nexusproto.TimeSeriesTile()
    #
    #             instance_dimension = next(
    #                 iter([dim for dim in ds[self.variable_to_read].dims if dim != self.time]))
    #
    #             tile.latitude.CopyFrom(
    #                 to_shaped_array(np.ma.filled(ds[self.latitude].data[dimtoslice[instance_dimension]], np.NaN)))
    #
    #             tile.longitude.CopyFrom(
    #                 to_shaped_array(
    #                     np.ma.filled(ds[self.longitude].data[dimtoslice[instance_dimension]], np.NaN)))
    #
    #             # Before we read the data we need to make sure the dimensions are in the proper order so we don't
    #             # have any indexing issues
    #             ordered_slices = slices_for_variable(ds, self.variable_to_read, dimtoslice)
    #             # Read data using the ordered slices, replacing masked values with NaN
    #             data_array = np.ma.filled(ds[self.variable_to_read].data[tuple(ordered_slices.values())], np.NaN)
    #
    #             tile.variable_data.CopyFrom(to_shaped_array(data_array))
    #
    #             if self.metadata is not None:
    #                 tile.meta_data.add().CopyFrom(
    #                     to_metadata(self.metadata, ds[self.metadata].data[tuple(ordered_slices.values())]))
    #
    #             tile.time.CopyFrom(
    #                 to_shaped_array(np.ma.filled(ds[self.time].data[dimtoslice[self.time]], np.NaN)))
    #
    #             output_tile.tile.time_series_tile.CopyFrom(tile)
    #
    #             yield output_tile
