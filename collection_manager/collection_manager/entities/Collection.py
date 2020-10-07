import os
from urllib.parse import urlparse
from dataclasses import dataclass
from datetime import datetime
from fnmatch import fnmatch
from glob import glob
from typing import List, Optional

from collection_manager.entities.exceptions import MissingValueCollectionError


@dataclass(frozen=True)
class Collection:
    dataset_id: str
    projection: str
    dimension_names: frozenset
    slices: frozenset
    path: str
    historical_priority: int
    forward_processing_priority: Optional[int] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None

    @staticmethod
    def from_dict(properties: dict):
        try:
            date_to = datetime.fromisoformat(properties['to']) if 'to' in properties else None
            date_from = datetime.fromisoformat(properties['from']) if 'from' in properties else None

            collection = Collection(dataset_id=properties['id'],
                                    projection=properties['projection'],
                                    dimension_names=frozenset(properties['dimensionNames'].items()),
                                    slices=frozenset(properties['slices'].items()),
                                    path=properties['path'],
                                    historical_priority=properties['priority'],
                                    forward_processing_priority=properties.get('forward-processing-priority', None),
                                    date_to=date_to,
                                    date_from=date_from)
            return collection
        except KeyError as e:
            raise MissingValueCollectionError(missing_value=e.args[0])

    def directory(self):
        if os.path.isdir(self.path):
            return self.path
        else:
            return os.path.dirname(self.path)

    def owns_file(self, file_path: str) -> bool:
        if urlparse(file_path).scheme == 's3':
            return file_path.find(self.path) == 0
        else:
            if os.path.isdir(file_path):
                raise IsADirectoryError()

            if os.path.isdir(self.path):
                return os.path.dirname(file_path) == self.path
            else:
                return fnmatch(file_path, self.path)
