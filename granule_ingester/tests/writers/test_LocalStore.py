from tkinter import W
import unittest
from os import path
import xarray as xr
import netCDF4
import json
from typing import Dict
from datetime import datetime
from pathlib import Path
#from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.processors.reading_processors import LazyLoadProcessor
from granule_ingester.writers import LocalStore
from dask.diagnostics import ProgressBar

def _build_solr_doc(self, ds: netCDF4._netCDF4) -> Dict:
    summary: TileSummary = tile.summary
    bbox: TileSummary.BBox = summary.bbox
    stats: TileSummary.DataStats = summary.stats

    min_time = datetime.strftime(datetime.utcfromtimestamp(stats.min_time), self.iso)
    max_time = datetime.strftime(datetime.utcfromtimestamp(stats.max_time), self.iso)
    day_of_year = datetime.utcfromtimestamp(stats.min_time).timetuple().tm_yday
    geo = self.determine_geo(bbox)

    granule_file_name: str = Path(summary.granule).name  # get base filename

    tile_type = tile.tile.WhichOneof("tile_type")
    tile_data = getattr(tile.tile, tile_type)

    var_names = json.loads(summary.data_var_name)
    standard_names = []
    if summary.standard_name:
        standard_names = json.loads(summary.standard_name)
    if not isinstance(var_names, list):
        var_names = [var_names]
    if not isinstance(standard_names, list):
        standard_names = [standard_names]

    input_document = {
        'table_s': self.TABLE_NAME,
        'geo': geo,
        'id': summary.tile_id,
        'solr_id_s': '{ds_name}!{tile_id}'.format(ds_name=summary.dataset_name, tile_id=summary.tile_id),
        'sectionSpec_s': summary.section_spec,
        'dataset_s': summary.dataset_name,
        'granule_s': granule_file_name,
        'tile_var_name_ss': var_names,
        'day_of_year_i': day_of_year,
        'tile_min_lon': bbox.lon_min,
        'tile_max_lon': bbox.lon_max,
        'tile_min_lat': bbox.lat_min,
        'tile_max_lat': bbox.lat_max,
        'tile_depth': tile_data.depth,
        'tile_min_time_dt': min_time,
        'tile_max_time_dt': max_time,
        'tile_min_val_d': stats.min,
        'tile_max_val_d': stats.max,
        'tile_avg_val_d': stats.mean,
        'tile_count_i': int(stats.count)
    }

    for var_name, standard_name in zip(var_names, standard_names):
        if standard_name:
            input_document[f'{var_name}.tile_standard_name_s'] = standard_name

    ecco_tile_id = getattr(tile_data, 'tile', None)
    if ecco_tile_id:
        input_document['ecco_tile'] = ecco_tile_id

    for attribute in summary.global_attributes:
        input_document[attribute.getName()] = attribute.getValues(
            0) if attribute.getValuesCount() == 1 else attribute.getValuesList()

    return input_document

def main():
    reading_processor_TROPOMI = LazyLoadProcessor(variable='[methane_mixing_ratio]',
                                                 latitude='lat',
                                                 longitude='lon',
                                                 time='time')
    granule_path_TROPOMI = path.join(path.dirname(__file__), '../granules/TROPOMI_methane_mixing_ratio_20200801.nc')
    #granule_path_OBP = path.join(path.dirname(__file__), '../granules/OBP_native_grid.nc')

    store_path = path.join(path.dirname(__file__), '../local_zarr_store/TROPOMI_MASTER.zarr') 
    zarr_writer_TROPOMI = LocalStore(store_path) 

    cdfDS = xr.open_dataset(granule_path_TROPOMI)
    chunkShape = (1,5,5)
    processed_granule = reading_processor_TROPOMI.process(cdfDS) 
    with ProgressBar(): 
        zarr_writer_TROPOMI.save_data(ds=processed_granule, cname='blosclz',
                                  clevel=9, shuffle=1, chunkShape=chunkShape)

    
if __name__ == "__main__":
    main()  
    