from abc import ABC, abstractmethod

import netCDF4

from granule_ingester.healthcheck import HealthCheck


class netCDF_Store(ABC):

    @abstractmethod
    def save_data(self, ds: netCDF4._netCDF4) -> None:
        pass
    