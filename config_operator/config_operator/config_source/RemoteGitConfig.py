import logging
import os
import sys
import time
from git import Repo
from typing import Callable
from .LocalDirConfig import LocalDirConfig

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

LISTEN_FOR_UPDATE_INTERVAL_SECONDS = 5
DEFAULT_LOCAL_REPO_DIR = os.path.join(sys.prefix, 'sdap', 'conf')


class RemoteGitConfig(LocalDirConfig):
    def __init__(self, git_url: str,
                 git_branch: str = 'master',
                 git_token: str = None,
                 update_every_seconds: int = LISTEN_FOR_UPDATE_INTERVAL_SECONDS,
                 local_dir: str = DEFAULT_LOCAL_REPO_DIR
                 ):
        """

        :param git_url:
        :param git_branch:
        :param git_token:
        """
        self._git_url = git_url if git_url.endswith(".git") else git_url + '.git'
        self._git_branch = git_branch
        self._git_token = git_token
        if local_dir is None:
            local_dir = DEFAULT_LOCAL_REPO_DIR
        self._update_every_seconds = update_every_seconds
        super().__init__(local_dir, update_every_seconds=self._update_every_seconds)
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

    def when_updated(self, callback: Callable[[], None]):
        """
        call function callback when the remote git repository is updated.
        """
        while True:
            time.sleep(self._update_every_seconds)
            remote_commit_key = self._pull_remote()
            if remote_commit_key != self._latest_commit_key:
                logger.info("remote git repository has been updated")
                callback()
                self._latest_commit_key = remote_commit_key
            else:
                logger.debug("remote git repository has not been updated")

        return None

