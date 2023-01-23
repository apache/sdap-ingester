# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
from urllib.parse import urlparse
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Optional

from botocore.compat import filter_ssl_warnings

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


class IngestionHistoryBuilder(ABC):
    @abstractmethod
    def build(self, dataset_id: str):
        pass


class GranuleStatus(Enum):
    DESIRED_FORWARD_PROCESSING = 1
    DESIRED_HISTORICAL = 2
    UNDESIRED = 3


class IngestionHistory(ABC):
    _signature_fun = None
    _latest_ingested_file_update: int = None

    async def push(self, file_path: str, modified_timestamp: int):
        """
        Record a file as having been ingested.
        :param file_path: The full path to the file to record.
        :return: None
        """
        file_name = IngestionHistory._get_standardized_path(file_path)
        signature = self._signature_fun(file_path) if self._signature_fun else str(modified_timestamp)
        await self._push_record(file_name, signature)

        if not self._latest_ingested_file_update:
            self._latest_ingested_file_update = modified_timestamp
        else:
            self._latest_ingested_file_update = max(self._latest_ingested_file_update, modified_timestamp)

        await self._save_latest_timestamp()

    async def get_granule_status(self,
                                 file_path: str,
                                 modified_timestamp: int,
                                 date_from: datetime = None,
                                 date_to: datetime = None) -> GranuleStatus:
        """
        Get the history status of a granule. DESIRED_FORWARD_PROCESSING means the granule has not yet been ingested
        and and is newer than the newest granule that was ingested (see IngestionHistory.latest_ingested_mtime).
        DESIRED_HISTORICAL means the granule has not yet been ingested, and is older than the newest granule that
        was ingested. UNDESIRED means either the granule has already been ingested, or it does not fall within the
        desired time range specified by date_fram and date_to.

        :param file_path: The full path to a granule to check the status of.
        :param date_from: An optional datetime defining the start of a desired time range within which the granule
                          should fall in order to be "desired".
        :param date_to: An optional datetime defining the end of a desired time range within which the granule
                        should fall in order to be "desired".
        :return: A GranuleStatus enum.
        """
        signature = self._signature_fun(file_path) if self._signature_fun else str(modified_timestamp)

        if self._in_time_range(modified_timestamp, start_date=self._latest_ingested_mtime()):
            return GranuleStatus.DESIRED_FORWARD_PROCESSING
        elif self._in_time_range(modified_timestamp, date_from, date_to) and not await self._already_ingested(file_path, signature):
            return GranuleStatus.DESIRED_HISTORICAL
        else:
            return GranuleStatus.UNDESIRED

    def _get_standardized_path(file_path: str):
        file_path = file_path.strip()
        # TODO: Why do we need to record the basename of the path, instead of just the full path?
        # The only reason this is here right now is for backwards compatibility to avoid triggering a full reingestion.
        if urlparse(file_path).scheme == 's3':
            return urlparse(file_path).path.strip("/")
        else:
            return os.path.basename(file_path)

    def _latest_ingested_mtime(self) -> Optional[datetime]:
        """
        Return the modified time of the most recently modified file that was ingested.
        :return: A datetime or None
        """
        if self._latest_ingested_file_update:
            return datetime.fromtimestamp(self._latest_ingested_file_update)
        else:
            return None

    async def _already_ingested(self, file_path: str, signature) -> bool:
        """
        Return a boolean indicating whether the specified file has already been ingested, based on its signature.
        :param file_path: The full path of a file to search for in the history.
        :return: A boolean indicating whether this file has already been ingested or not
        """
        file_name = IngestionHistory._get_standardized_path(file_path)
        return signature == await self._get_signature(file_name)

    @abstractmethod
    async def _save_latest_timestamp(self):
        pass

    @abstractmethod
    async def _push_record(self, file_name, signature):
        pass

    @abstractmethod
    async def _get_signature(self, file_name):
        pass

    @staticmethod
    def _in_time_range(timestamp: int, start_date: datetime = None, end_date: datetime = None):
        """
        :param file: file path as a string
        :param date_from: timestamp, can be None
        :param date_to: timestamp, can be None
        :return: True is the update time of the file is between ts_from and ts_to. False otherwise
        """
        is_after_from = int(start_date.timestamp()) < timestamp if start_date else True
        is_before_to = int(end_date.timestamp()) > timestamp if end_date else True

        return is_after_from and is_before_to
