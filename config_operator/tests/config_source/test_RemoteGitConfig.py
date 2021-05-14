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
