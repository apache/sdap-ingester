import pysolr
import requests
import logging
import ctypes
from unittest.mock import MagicMock

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def doc_key(dataset_id, file_name):
    return ctypes.c_size_t(hash(f'{dataset_id}{file_name}')).value


class SolrIngestionHistory:
    _solr = None
    _collection_name = "nexusgranules"
    _solr_url = None
    _req_session = None

    def __init__(self, solr_url):
        if not self._solr:
            self._solr_url = solr_url
            self.create_collection_if_needed()
            self._solr = pysolr.Solr(f'{solr_url}/{self._collection_name}')



    def create_collection_if_needed(self):
        if not self._req_session:
            self._req_session = requests.session()

        payload = {'action': 'CLUSTERSTATUS'}
        result = self._req_session.get(f'{self._solr_url}/admin/collections', params=payload)
        response = result.json()
        node_number = len(response['cluster']['live_nodes'])

        if self._collection_name not in response['cluster']['collections'].keys():
            # Create collection
            payload = {'action': 'CREATE',
                       'name': self._collection_name,
                       'numShards': node_number
                       }
            result = self._req_session.get(f'{self._solr_url}/admin/collections', params=payload)
            response = result.json()
            logger.info(f"solr collection created {response}")
        else:
            logger.info(f"collection {self._collection_name} already exists")


        # Update schema
        schema_url = f'{self._solr_url}/solr/{self._collection_name}/schema'
        # granule_s # dataset_s so that all the granule of a dataset are less likely to be on the same shard
        #self.add_unique_key_field(schema_url, "uniqueKey_s", "StrField")
        self.add_str_field(schema_url, "dataset_s", "StrField")
        self.add_str_field(schema_url, "granule_s", "StrField")
        self.add_str_field(schema_url, "granule_md5sum_s", "StrField")

    def push(self, dataset_id, file_name, md5sum):
        hash_id = doc_key(dataset_id, file_name)
        self._solr.delete(q=f"id:{hash_id}")
        self._solr.add([{
            'id': hash_id,
            'dataset_s': dataset_id,
            'granule_s': file_name,
            'granule_md5sum_s': md5sum}])
        self._solr.commit()
        return None

    def get(self, dataset_id, file_name):
        hash_id = doc_key(dataset_id, file_name)
        results = self._solr.search(q=f"id:{hash_id}")
        return results

    def get_md5sum(self, dataset_id, file_name):
        results = self.get(dataset_id, file_name)
        return results.docs[0]['granule_md5sum_s']


    # helper
    def add_str_field(self, schema_url, field_name, field_type):
        '''
        :param schema_url:
        :param field_name:
        :param field_type
        :return:
        '''
        add_field_payload = {
            "add-field": {
                "name": field_name,
                "type": field_type,
                "stored": False
            }
        }
        result = self._req_session.post(schema_url, data=add_field_payload)

    def add_str_field(self, schema_url, field_name, field_type):
        '''
        :param schema_url:
        :param field_name:
        :param field_type
        :return:
        '''
        add_field_payload = {
            "add-field": {
                "name": field_name,
                "type": field_type,
                "stored": False
            }
        }
        result = self._req_session.post(schema_url, data=add_field_payload)
