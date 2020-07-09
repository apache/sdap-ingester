from abc import ABC, abstractmethod

from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.healthcheck import HealthCheck


class DataStore(HealthCheck, ABC):


    @abstractmethod
    def save_data(self, nexus_tile: nexusproto.NexusTile) -> None:
        pass

