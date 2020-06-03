import unittest
import os

from flask import Flask
from flask_restplus import Api

from sdap_ingest_manager.config.ConfigMap import ConfigMap
from sdap_ingest_manager.ingestion_order_store.FileIngestionOrderStore import FileIngestionOrderStore
from sdap_ingest_manager.ingestion_order_store.templates import Templates

flask_app = Flask(__name__)
app = Api(app=flask_app)
templates = Templates(app)

class ConfigMapTest(unittest.TestCase):
    def test_createconfigmap(self):

        test_ingestion_order_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                 '..',
                                                 'resources',
                                                 'data',
                                                 'collections.yml')
        file_ingestion_order_store = FileIngestionOrderStore(path=test_ingestion_order_file,
                                                             order_template=templates.order_template)
        
        config_map = ConfigMap('collections.yml', 'sdap', file_ingestion_order_store)
        config_map.publish()



if __name__ == '__main__':
    unittest.main()
