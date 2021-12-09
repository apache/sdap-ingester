from abc import ABC, abstractmethod

from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.healthcheck import HealthCheck


class MetadataStore(HealthCheck, ABC):
    @abstractmethod
    def save_metadata(self, nexus_tile: nexusproto.NexusTile) -> None:
        pass

    @abstractmethod
    def commit(self) -> None:
        pass
