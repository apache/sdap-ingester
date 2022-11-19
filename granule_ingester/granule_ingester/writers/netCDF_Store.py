from abc import ABC, abstractmethod

import xarray as xr

from granule_ingester.healthcheck import HealthCheck


class netCDF_Store(ABC):

    @abstractmethod
    def save_data(self, ds: xr.Dataset) -> None:
        pass
    