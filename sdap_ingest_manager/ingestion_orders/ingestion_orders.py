import os
import sys
import logging
import yaml
from flask_restplus import marshal
from git import Repo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



class IngestionOrders:
    _ingestion_orders = {}

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
        self._git_url = git_url = git_url if git_url.endswith(".git") else git_url + '.git'
        self._git_branch = git_branch
        self._git_token = git_token
        self._order_template = order_template
        self._local_dir = os.path.join(sys.prefix, 'sdap', 'conf')
        self._file_name = os.path.join(self._local_dir, 'ingestion_orders.yml')
        self._repo = None
        self._init_local_config_repo()
        self._read_from_file()

    def get_git_url(self):
        return self._repo.remotes.origin.url

    def get_git_branch(self):
        return self._repo.active_branch.name

    def pull(self):
        o = self._repo.remotes.origin
        o.pull()
        self._read_from_file()

    def dump(self):
        return self._ingestion_orders

    def _init_local_config_repo(self):
        self._repo = Repo.init(self._local_dir)
        if len(self._repo.remotes) == 0 or 'origin' not in [r.name for r in self._repo.remotes]:
            self._repo.create_remote('origin', self._git_url)
        self._repo.git.fetch()
        self._repo.git.checkout(self._git_branch)

    def _read_from_file(self):
        try:
            with open(self._file_name, 'r') as f:
                orders_yml = yaml.load(f, Loader=yaml.FullLoader)

            ingestion_orders = {}
            for (c_id, ingestion_order) in orders_yml.items():
                ingestion_order['id'] = c_id
                ingestion_orders[c_id] = marshal(ingestion_order, self._order_template)

            logger.info(f"collections are {ingestion_orders}")
            self._ingestion_orders = ingestion_orders

        except FileNotFoundError:
            logger.error(f"no collection configuration found at {self._ingestion_orders}")
