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

import asyncio
import os
import time
import logging
from functools import partial
import yaml
from typing import Callable

from config_operator.config_source.exceptions import UnreadableFileException

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

LISTEN_FOR_UPDATE_INTERVAL_SECONDS = 1


class LocalDirConfig:

    def __init__(self, local_dir: str,
                 update_every_seconds: float = LISTEN_FOR_UPDATE_INTERVAL_SECONDS,
                 update_date_fun=os.path.getmtime):
        logger.info(f'create config on local dir {local_dir}')
        self._local_dir = local_dir
        self._update_date_fun = update_date_fun
        self._update_every_seconds = update_every_seconds
        self._latest_update = self._get_latest_update()

    def get_files(self):
        files = []
        for f in os.listdir(self._local_dir):
            if os.path.isfile(os.path.join(self._local_dir, f)) \
                    and 'README' not in f \
                    and not f.startswith('.'):
                files.append(f)

        return files

    def _test_read_yaml(self, file_name: str):
        """ check yaml syntax raise yaml.parser.ParserError is it doesn't"""
        with open(os.path.join(self._local_dir, file_name), 'r') as f:
            docs = yaml.load_all(f, Loader=yaml.FullLoader)
            for doc in docs:
                pass

    def get_file_content(self, file_name: str):
        logger.info(f'read configuration file {file_name}')
        try:
            self._test_read_yaml(file_name)
            with open(os.path.join(self._local_dir, file_name), 'r') as f:
                return f.read()
        except UnicodeDecodeError as e:
            raise UnreadableFileException(e)
        except yaml.parser.ParserError as e:
            raise UnreadableFileException(e)
        except yaml.scanner.ScannerError as e:
            raise UnreadableFileException(e)

    def _get_latest_update(self):
        m_times = [self._update_date_fun(root) for root, _, _ in os.walk(self._local_dir)]
        if m_times:
            return max(m_times)
        else:
            return None

    async def when_updated(self, callback: Callable[[], None], loop=None):
        """
          call function callback when the local directory is updated.
        """
        if loop is None:
            loop = asyncio.get_running_loop()

        latest_update = self._get_latest_update()
        if latest_update is None or (latest_update > self._latest_update):
            logger.info("local config dir has been updated")
            callback()
        else:
            logger.debug("local config dir has not been updated")
        loop.call_later(self._update_every_seconds, partial(self.when_updated, callback, loop))

        return None
