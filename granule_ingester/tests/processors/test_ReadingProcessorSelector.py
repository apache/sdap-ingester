
import unittest
from os import path


import xarray as xr


from granule_ingester.processors import ReadingProcessorSelector
from granule_ingester.processors.reading_processors import GridReadingProcessor, EccoReadingProcessor, TimeSeriesReadingProcessor, SwathReadingProcessor


from granule_ingester.processors.ReadingProcessorSelector import GRID_PROCESSORS


class TestGenerateTileId(unittest.TestCase):

    def test_detect_dimensions(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/SMAP_L2B_SSS_04892_20160101T005507_R13080.h5')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            selector = ReadingProcessorSelector(dataset, 'smap_sss')
            self.assertEqual(('lat', 'lon', 'row_time'), selector.detect_dimensions())

    def test_detect_grid_type_smap(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/SMAP_L2B_SSS_04892_20160101T005507_R13080.h5')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            selector = ReadingProcessorSelector(dataset, 'smap_sss')
            processor = selector.detect_grid_type('lat', 'lon', 'time', GRID_PROCESSORS)
            self.assertEqual(GridReadingProcessor, processor)

    def test_detect_grid_type_ecco_native(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/OBP_native_grid.nc')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            selector = ReadingProcessorSelector(dataset, 'OBP')
            processor = selector.detect_grid_type('YC', 'XC', 'time', GRID_PROCESSORS)
            self.assertEqual(EccoReadingProcessor, processor)

    def test_detect_grid_type_ecco_interp(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/OBP_2017_01.nc')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            selector = ReadingProcessorSelector(dataset, 'OBP')
            processor = selector.detect_grid_type('latitude', 'longitude', 'time', GRID_PROCESSORS)
            self.assertEqual(GridReadingProcessor, processor)

    def test_detect_grid_type_time_series(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/not_empty_wswm.nc')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            selector = ReadingProcessorSelector(dataset, 'Qout')
            processor = selector.detect_grid_type('lat', 'lon', 'time', GRID_PROCESSORS)
            self.assertEqual(TimeSeriesReadingProcessor, processor)

    def test_detect_grid_type_swatch(self):
        netcdf_path = path.join(path.dirname(__file__), '../granules/not_empty_smap.h5')
        with xr.open_dataset(netcdf_path, decode_cf=True) as dataset:
            selector = ReadingProcessorSelector(dataset, 'smap_sss')
            processor = selector.detect_grid_type('lat', 'lon', 'row_time', GRID_PROCESSORS)
            self.assertEqual(SwathReadingProcessor, processor)
