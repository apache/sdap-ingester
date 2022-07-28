from tkinter import W
import unittest
from os import path
import xarray as xr
#from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.processors.reading_processors import LazyLoadProcessor
from granule_ingester.writers import LocalStore
from dask.diagnostics import ProgressBar

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