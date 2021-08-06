import json
import unittest
from os import path

import xarray as xr
from granule_ingester.processors import TileSummarizingProcessor
from granule_ingester.processors.reading_processors import GridMultiBandReadingProcessor
from granule_ingester.processors.reading_processors.GridReadingProcessor import GridReadingProcessor
from nexusproto import DataTile_pb2 as nexusproto


class TestTileSummarizingProcessor(unittest.TestCase):
    def test_standard_name_exists_01(self):
        """
        Test that the standard_name attribute exists in a
        Tile.TileSummary object after being processed with
        TileSummarizingProcessor
        """
        reading_processor = GridReadingProcessor(
            variable='analysed_sst',
            latitude='lat',
            longitude='lon',
            time='time',
            tile='tile'
        )
        relative_path = '../granules/20050101120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.0.nc'
        granule_path = path.join(path.dirname(__file__), relative_path)
        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path
        tile_summary.data_var_name = json.dumps('analysed_sst')

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dims = {
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            'time': slice(0, 1),
            'tile': slice(10, 11),
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dims, input_tile)
            tile_summary_processor = TileSummarizingProcessor('test')
            new_tile = tile_summary_processor.process(tile=output_tile, dataset=ds)
            self.assertEqual('"sea_surface_temperature"', new_tile.summary.standard_name, f'wrong new_tile.summary.standard_name')

    def test_hls_single_var01(self):
        """
        Test that the standard_name attribute exists in a
        Tile.TileSummary object after being processed with
        TileSummarizingProcessor
        """
        input_var_list = [f'B{k:02d}' for k in range(1, 12)]
        input_var_list = ['B01']
        reading_processor = GridReadingProcessor(input_var_list, 'lat', 'lon', time='time', tile='tile')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path
        tile_summary.data_var_name = json.dumps(input_var_list)

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            'tile': slice(10, 11),
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            tile_summary_processor = TileSummarizingProcessor('test')
            new_tile = tile_summary_processor.process(tile=output_tile, dataset=ds)
            self.assertTrue(abs(new_tile.summary.stats.mean - 0.26137) < 0.001, f'mean value is not close expected: 0.26137. actual: {new_tile.summary.stats.mean}')

    def test_hls_multiple_var_01(self):
        """
        Test that the standard_name attribute exists in a
        Tile.TileSummary object after being processed with
        TileSummarizingProcessor
        """
        input_var_list = [f'B{k:02d}' for k in range(1, 12)]
        reading_processor = GridMultiBandReadingProcessor(input_var_list, 'lat', 'lon', time='time', tile='tile')
        granule_path = path.join(path.dirname(__file__), '../granules/HLS.S30.T11SPC.2020001.v1.4.hdf.nc')

        tile_summary = nexusproto.TileSummary()
        tile_summary.granule = granule_path
        tile_summary.data_var_name = json.dumps(input_var_list)

        input_tile = nexusproto.NexusTile()
        input_tile.summary.CopyFrom(tile_summary)

        dimensions_to_slices = {
            'time': slice(0, 1),
            'lat': slice(0, 30),
            'lon': slice(0, 30),
            'tile': slice(10, 11),
        }

        with xr.open_dataset(granule_path, decode_cf=True) as ds:
            output_tile = reading_processor._generate_tile(ds, dimensions_to_slices, input_tile)
            tile_summary_processor = TileSummarizingProcessor('test')
            new_tile = tile_summary_processor.process(tile=output_tile, dataset=ds)
            self.assertTrue(abs(new_tile.summary.stats.mean - 0.26523) < 0.001, f'mean value is not close expected: 0.26523. actual: {new_tile.summary.stats.mean}')