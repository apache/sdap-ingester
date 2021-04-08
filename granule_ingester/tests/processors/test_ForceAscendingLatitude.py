import unittest

import xarray as xr
import numpy as np
from os import path
from nexusproto import DataTile_pb2 as nexusproto

from nexusproto.serialization import from_shaped_array, to_shaped_array

from granule_ingester.processors.ForceAscendingLatitude import ForceAscendingLatitude
from granule_ingester.processors.reading_processors.GridReadingProcessor import GridReadingProcessor

class TestForceAscendingLatitude(unittest.TestCase):

    def read_tile(self):
        reading_processor = GridReadingProcessor('B03', 'lat', 'lon', time='time')
        granule_path = path.join(path.dirname(__file__),
                                 '/Users/loubrieu/Documents/sdap/HLS/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')
        input_tile = nexusproto.NexusTile()
        input_tile.summary.granule = granule_path

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30)
        }

        with xr.open_dataset(granule_path) as ds:
            return reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)

    def test_process(self):
        processor = ForceAscendingLatitude()

        tile = self.read_tile()

        tile_type = tile.tile.WhichOneof("tile_type")
        tile_data = getattr(tile.tile, tile_type)
        latitudes = from_shaped_array(tile_data.latitude)
        variable_data = from_shaped_array(tile_data.variable_data)
        print(latitudes)
        print(variable_data)


        flipped_tile = processor.process(tile)

        the_flipped_tile_type = flipped_tile.tile.WhichOneof("tile_type")
        the_flipped_tile_data = getattr(flipped_tile.tile, the_flipped_tile_type)

        flipped_latitudes = from_shaped_array(the_flipped_tile_data.latitude)
        flipped_data = from_shaped_array(the_flipped_tile_data.variable_data)

        print(flipped_latitudes[1])
        np.testing.assert_almost_equal(flipped_latitudes[1], 38.72608, decimal=5, err_msg='', verbose=True)
        print(flipped_data[1,1])
        np.testing.assert_almost_equal(flipped_data[1,1], 0.3116, decimal=4, err_msg='', verbose=True)



