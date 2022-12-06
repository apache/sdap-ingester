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
import logging
import os
import sys
from functools import partial
from typing import Callable

from git import Repo

from .LocalDirConfig import LocalDirConfig

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

LISTEN_FOR_UPDATE_INTERVAL_SECONDS = 5
DEFAULT_LOCAL_REPO_DIR = os.path.join(sys.prefix, 'sdap', 'conf')


class RemoteGitConfig(LocalDirConfig):
    def __init__(self, git_url: str,
                 git_branch: str = 'master',
                 git_username: str = None,
                 git_token: str = None,
                 update_every_seconds: float = LISTEN_FOR_UPDATE_INTERVAL_SECONDS,
                 local_dir: str = DEFAULT_LOCAL_REPO_DIR,
                 repo: Repo = None):
        """

        :param git_url:
        :param git_branch:
        :param git_token:
        """
        self._git_url = git_url if git_url.endswith(".git") else git_url + '.git'
        if git_username and git_token:
            self._git_url.replace('https://', f'https://{git_username}:{git_token}')
            self._git_url.replace('http://', f'http://{git_username}:{git_token}')

        self._git_branch = git_branch
        self._git_token = git_token
        if local_dir is None:
            local_dir = DEFAULT_LOCAL_REPO_DIR
        super().__init__(local_dir, update_every_seconds=update_every_seconds)


        if repo:
            self._repo = repo
        else:
            self._repo = None
            self._init_local_config_repo()

        self._latest_commit_key = self._pull_remote()

    def _pull_remote(self):
        o = self._repo.remotes.origin
        res = o.pull()
        return res[0].commit.hexsha  # return the latest commit key

    def _init_local_config_repo(self):
        self._repo = Repo.init(self._local_dir)
        if len(self._repo.remotes) == 0 or 'origin' not in [r.name for r in self._repo.remotes]:
            self._repo.create_remote('origin', self._git_url)
        self._repo.git.fetch()
        self._repo.git.checkout(self._git_branch)

    async def when_updated(self, callback: Callable[[], None], loop=None):
        """
        call function callback when the remote git repository is updated.
        """
        if loop is None:
            loop = asyncio.get_running_loop()

        remote_commit_key = self._pull_remote()
        if remote_commit_key != self._latest_commit_key:
            logger.info("remote git repository has been updated")
            callback()
            self._latest_commit_key = remote_commit_key
        else:
            logger.debug("remote git repository has not been updated")

        loop.call_later(self._update_every_seconds, partial(self.when_updated, callback, loop))

        return None
