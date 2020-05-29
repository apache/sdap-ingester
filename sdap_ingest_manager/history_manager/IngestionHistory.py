import hashlib
import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

BLOCK_SIZE = 65536


def md5sum_from_filepath(file_path):
    hasher = hashlib.md5()
    with open(file_path.strip(), 'rb') as afile:
        buf = afile.read(BLOCK_SIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCK_SIZE)
    return hasher.hexdigest()


class IngestionHistory(ABC):
    _signature_fun = None
    _latest_ingested_file_update = None

    def push(self, file_path):
        file_path = file_path.strip()
        file_name = os.path.basename(file_path)
        signature = self._signature_fun(file_path)
        self._push_record(file_name, signature)

        if not self._latest_ingested_file_update:
            self._latest_ingested_file_update = os.path.getmtime(file_path)
        else:
            self._latest_ingested_file_update = max(self._latest_ingested_file_update, os.path.getmtime(file_path))

    def has_valid_cache(self, file_path):
        file_path = file_path.strip()
        file_name = os.path.basename(file_path)
        signature = self._signature_fun(file_path)
        logger.debug(f"compare {signature} with {self._get_signature(file_name)}")
        return signature == self._get_signature(file_name)

    def get_latest_ingested_file_update(self):
        return self._latest_ingested_file_update

    @abstractmethod
    def _push_record(self, file_name, signature):
        pass

    @abstractmethod
    def _get_signature(self, file_name):
        pass
