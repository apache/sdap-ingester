import unittest
from collections import OrderedDict
from os import path

import xarray as xr

from granule_ingester.processors.reading_processors import TileReadingProcessor


class TestEccoReadingProcessor(unittest.TestCase):

    def test_slices_for_variable(self):
        dimensions_to_slices = {
            'j': slice(0, 1),
            'tile': slice(0, 1),
            'i': slice(0, 1),
            'time': slice(0, 1)
        }

        expected = {
            'tile': slice(0, 1, None),
            'j': slice(0, 1, None),
            'i': slice(0, 1, None)
        }

        granule_path = path.join(path.dirname(__file__), '../granules/OBP_native_grid.nc')
        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            slices = TileReadingProcessor._slices_for_variable(ds['XC'], dimensions_to_slices)
            self.assertEqual(slices, expected)
