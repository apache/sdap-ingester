import logging
from abc import ABC, abstractmethod

import yaml
from flask_restplus import marshal

logger = logging.getLogger(__name__)


class IngestionOrderStore(ABC):
    _ingestion_orders = {}
    _file_name = None

    def __init__(self, order_template):
        self._order_template = order_template

    @abstractmethod
    def load(self):
        pass

    def orders(self):
        return list(self._ingestion_orders.values())

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
