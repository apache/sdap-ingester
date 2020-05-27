import logging
import os
import sys

from git import Repo

from sdap_ingest_manager.ingestion_order_store.IngestionOrderStore import IngestionOrderStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GitIngestionOrderStore(IngestionOrderStore):

    def __init__(self, git_url,
                 git_branch='master',
                 git_token=None,
                 order_template=None
                 ):
        """

        :param git_url:
        :param git_branch:
        :param git_token:
        :param order_template: order_template coming from flask-restplus api to marshall the order
        """
        self._git_url = git_url if git_url.endswith(".git") else git_url + '.git'
        self._git_branch = git_branch
        self._git_token = git_token
        self._local_dir = os.path.join(sys.prefix, 'sdap', 'conf')
        self._file_name = os.path.join(self._local_dir, 'ingestion_order_store.yml')
        self._repo = None

        super().__init__(order_template)

        self._init_local_config_repo()

    def get_git_url(self):
        return self._repo.remotes.origin.url

    def get_git_branch(self):
        return self._repo.active_branch.name

    def load(self):
        self._repo.remotes.origin.pull()
        self._read_from_file()

    def _init_local_config_repo(self):
        self._repo = Repo.init(self._local_dir)
        if len(self._repo.remotes) == 0 or 'origin' not in [r.name for r in self._repo.remotes]:
            self._repo.create_remote('origin', self._git_url)
        self._repo.git.fetch()
        self._repo.git.checkout(self._git_branch)
