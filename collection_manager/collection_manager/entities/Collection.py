import os
from datetime import datetime
from fnmatch import fnmatch
from glob import glob
from typing import NamedTuple, List


class Collection(NamedTuple):
    dataset_id: str
    variable: str
    path: str
    historical_priority: int
    forward_processing_priority: int
    date_from: datetime
    date_to: datetime

    @staticmethod
    def from_dict(properties: dict):
        date_to = datetime.fromisoformat(properties['to']) if 'to' in properties else None
        date_from = datetime.fromisoformat(properties['from']) if 'from' in properties else None

        collection = Collection(dataset_id=properties['id'],
                                variable=properties['variable'],
                                path=properties['path'],
                                historical_priority=properties['priority'],
                                forward_processing_priority=properties.get('forward_processing_priority',
                                                                           properties['priority']),
                                date_to=date_to,
                                date_from=date_from)
        return collection

    def directory(self):
        if os.path.isdir(self.path):
            return self.path
        else:
            return os.path.dirname(self.path)

    def owns_file(self, file_path: str) -> bool:
        if os.path.isdir(file_path):
            raise IsADirectoryError()

        if os.path.isdir(self.path):
            return os.path.dirname(file_path) == self.path
        else:
            return fnmatch(file_path, self.path)

    def files_owned(self) -> List[str]:
        return glob(self.path)
