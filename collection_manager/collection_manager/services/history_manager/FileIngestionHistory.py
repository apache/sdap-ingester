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

import logging
import os
from pathlib import Path

from collection_manager.services.history_manager.IngestionHistory import IngestionHistory
from collection_manager.services.history_manager.IngestionHistory import IngestionHistoryBuilder

logger = logging.getLogger(__name__)


class FileIngestionHistoryBuilder(IngestionHistoryBuilder):
    def __init__(self, history_path: str, signature_fun=None):
        self._history_path = history_path
        self._signature_fun = signature_fun

    def build(self, dataset_id: str):
        return FileIngestionHistory(history_path=self._history_path,
                                    dataset_id=dataset_id,
                                    signature_fun=self._signature_fun)


# TODO: clean this up, better tests
class FileIngestionHistory(IngestionHistory):

    def __init__(self, history_path: str, dataset_id: str, signature_fun=None):
        """
        Constructor
        :param history_path:
        :param dataset_id:
        :param signature_fun: function which creates the signature of the cache,
                              a file path string as argument and returns a string (md5sum, time stamp)
        """
        self._dataset_id = dataset_id
        self._history_file_path = os.path.join(history_path, f'{dataset_id}.csv')
        self._signature_fun = signature_fun
        self._history_dict = {}
        self._load_history_dict()

        Path(history_path).mkdir(parents=True, exist_ok=True)
        self._history_file = open(f"{self._history_file_path}", 'a', buffering=1)

        self._latest_ingested_file_update_file_path = os.path.join(history_path, f'{dataset_id}.ts')
        if os.path.exists(self._latest_ingested_file_update_file_path):
            with open(self._latest_ingested_file_update_file_path, 'r') as f_ts:
                self._latest_ingested_file_update = float(f_ts.readline())

    def _load_history_dict(self):
        try:
            with open(self._history_file_path, 'r') as f_history:
                for line in f_history:
                    filename, md5sum = line.strip().split(',')
                    self._history_dict[filename] = md5sum
        except FileNotFoundError:
            logger.info("No history file created yet")

    def __del__(self):
        self._history_file.close()
        self._purge()
        del self._history_dict

    def reset_cache(self):
        try:
            os.remove(self._history_file_path)
            logger.info(f"history cache {self._history_file_path} removed")
        except FileNotFoundError:
            logger.info(f"history cache {self._history_file_path} does not exist, does not need to be removed")

    async def _save_latest_timestamp(self):
        if self._latest_ingested_file_update:
            with open(self._latest_ingested_file_update_file_path, 'w') as f_ts:
                f_ts.write(f'{str(self._latest_ingested_file_update)}\n')

    def _purge(self):
        logger.info("purge the history file from duplicates")
        unique_file_names = set()
        try:
            with open(f"{self._history_file_path}.buff", "w") as f:
                history_file = open(self._history_file_path, "r")
                for line in reversed(list(history_file)):
                    file_name = line.split(",")[0]
                    if file_name not in unique_file_names:
                        unique_file_names.add(file_name)
                        f.write(line)
                    else:
                        logger.info(f"skip file {file_name} in purge")
                history_file.close()
            logger.info(f"purge done in file {self._history_file_path}.buff replace in {self._history_file_path}")
            os.replace(f"{self._history_file_path}.buff", self._history_file_path)
        except FileNotFoundError:
            logger.info(f"no history file {self._history_file_path} to purge")

    async def _push_record(self, file_name, signature):
        self._history_dict[file_name] = signature

        self._history_file.write(f'{file_name},{signature}\n')

    async def _get_signature(self, file_name):
        return self._history_dict.get(file_name, None)
