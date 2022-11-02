from abc import ABC, abstractmethod

from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.healthcheck import HealthCheck

from typing import List


class MetadataStore(HealthCheck, ABC):
    @abstractmethod
    def save_metadata(self, nexus_tile: nexusproto.NexusTile) -> None:
        pass

    @abstractmethod
    def save_batch(self, tiles: List[nexusproto.NexusTile]) -> None:
        pass

