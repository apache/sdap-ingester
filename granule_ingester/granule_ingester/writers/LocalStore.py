# LocalStore
# Purpose: Writes netCDF4 file into a machine local directory
# Written by: Ricky Fok
from os import path
import asyncio
import logging
from typing import Tuple
#from msilib.schema import Error
import traceback
import logging
import xarray as xr
from numcodecs import Blosc

from granule_ingester.exceptions import CassandraFailedHealthCheckError, CassandraLostConnectionError
from granule_ingester.writers import netCDF_Store

logger = logging.getLogger(__name__)

class LocalStore(netCDF_Store):
    def __init__(self, store_path: str):
        self.store_path = store_path 

    #async def health_check(self) -> bool:
        #try:
            #session = self._get_session()
            #session.shutdown()
            #return True
        #except Exception:
            #raise CassandraFailedHealthCheckError("Cannot connect to Cassandra!")

    def save_data(self, ds: xr.Dataset, cname: str, clevel: int, shuffle: int, chunkShape: Tuple[int, int, int]) -> None: 
        compressor = Blosc(cname = cname, clevel = clevel, shuffle = shuffle)
        # @TODO be able to customize encoding for each dava variable
        encoding = {vname: {'compressor': compressor, 'chunks': chunkShape} for vname in ds.data_vars}     

        try:
            ds.to_zarr(self.store_path, encoding = encoding, consolidated=True , mode='w')
        # @TODO update metadata in master Zarr and place error checks for invalid paths.
        except Exception as e:
            logging.error(traceback.format_exc())
            