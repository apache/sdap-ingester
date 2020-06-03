import logging
import os
import sys
import time
from git import Repo, Remote
from sdap_ingest_manager.config import LocalDirConfig


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

LISTEN_FOR_UPDATE_INTERVAL_SECONDS = 5

class RemoteGitConfig(LocalDirConfig):
    def __init__(self, git_url,
                 git_branch='master',
                 git_token=None
                 ):
        """

        :param git_url:
        :param git_branch:
        :param git_token:
        """
        self._git_url = git_url if git_url.endswith(".git") else git_url + '.git'
        self._git_branch = git_branch
        self._git_token = git_token
        local_dir = os.path.join(sys.prefix, 'sdap', 'conf')
        super().__init__(local_dir)
        self._repo = None
        self._init_local_config_repo()
        self._latest_commit_key = self._repo.head.commit.hexsha

    def _pull_remote(self):
        o = self._repo.remotes.origin
        res = o.pull()
        return res[0].commit.hexsha # return the latest commit key

    def _init_local_config_repo(self):
        self._repo = Repo.init(self._local_dir)
        if len(self._repo.remotes) == 0 or 'origin' not in [r.name for r in self._repo.remotes]:
            self._repo.create_remote('origin', self._git_url)
        self._repo.git.fetch()
        self._repo.git.checkout(self._git_branch)

    def when_updated(self, callback):

        while True:
            time.sleep(LISTEN_FOR_UPDATE_INTERVAL_SECONDS)
            remote_commit_key = self._pull_remote()
            if remote_commit_key != self._latest_commit_key:
                logger.info("remote git repository has been updated")
                callback()
                self._latest_commit_key = remote_commit_key
            else:
                logger.debug("remote git repository has not been updated")
            
            
                    
        
    
        
