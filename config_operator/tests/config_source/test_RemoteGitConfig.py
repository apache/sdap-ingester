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
import tempfile
import time
import unittest
from unittest.mock import Mock

from config_operator.config_source import RemoteGitConfig


class TestRemoteDirConfig(unittest.TestCase):
    _latest_commit = 0

    def _get_origin(self):
        commit = Mock()
        commit.hexsha = self._latest_commit
        self._latest_commit += 1

        pull_result = Mock()
        pull_result.commit = commit

        return [pull_result]

    def test_when_updated(self):
        origin_branch = Mock()
        origin_branch.pull = self._get_origin

        remotes = Mock()
        remotes.origin = origin_branch

        repo = Mock()
        repo.remotes = remotes

        git_config = RemoteGitConfig(git_url='https://github.com/tloubrieu-jpl/sdap-ingester-config',
                                     update_every_seconds=0.25,
                                     local_dir=os.path.join(tempfile.gettempdir(), 'sdap'),
                                     repo=repo)

        callback = Mock()

        asyncio.run(git_config.when_updated(callback))

        time.sleep(2)

        assert callback.called


if __name__ == '__main__':
    unittest.main()
