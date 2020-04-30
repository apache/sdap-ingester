import os
import pysolr
import requests
import logging
import ctypes



logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def doc_key(dataset_id, file_name):
    return ctypes.c_size_t(hash(f'{dataset_id}{file_name}')).value


class DatasetIngestionHistorySolr:
    _solr = None
    _collection_name = "nexusgranules"
    _solr_url = None
    _req_session = None
    _dataset_id = None
    _signature_fun = None

    def __init__(self, solr_url, dataset_id, signature_fun):
        self._solr_url = solr_url
        self._create_collection_if_needed()
        self._solr = pysolr.Solr(f'{solr_url}/{self._collection_name}')
        self._dataset_id = dataset_id
        self._signature_fun = signature_fun

    def __del__(self):
        self._req_session.close()

    def push(self, file_path):
        file_path = file_path.strip()
        file_name = os.path.basename(file_path)
        signature = self._signature_fun(file_path)
        self._push_record(file_name, signature)

    def has_valid_cache(self, file_path):
        file_path = file_path.strip()
        file_name = os.path.basename(file_path)
        signature = self._signature_fun(file_path)
        logger.debug(f"compare {signature} with {self._get_signature(file_name)}")
        return signature == self._get_signature(file_name)

    def _push_record(self, file_name, signature):
        hash_id = doc_key(self._dataset_id, file_name)
        self._solr.delete(q=f"id:{hash_id}")
        self._solr.add([{
            'id': hash_id,
            'dataset_s': self._dataset_id,
            'granule_s': file_name,
            'granule_signature_s': signature}])
        self._solr.commit()
        return None

    def _get_record(self, file_name):
        hash_id = doc_key(self._dataset_id, file_name)
        results = self._solr.search(q=f"id:{hash_id}")
        return results

    def _get_signature(self, file_name):
        results = self._get_record(file_name)
        if results:
            return results.docs[0]['granule_signature_s']
        else:
            return None

    def _create_collection_if_needed(self):
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
        # self.add_unique_key_field(schema_url, "uniqueKey_s", "StrField")
        self._add_str_field(schema_url, "dataset_s", "StrField")
        self._add_str_field(schema_url, "granule_s", "StrField")
        self._add_str_field(schema_url, "granule_md5sum_s", "StrField")

    def _add_str_field(self, schema_url, field_name, field_type):
        """
        Helper to add a string field in a solr schema
        :param schema_url:
        :param field_name:
        :param field_type
        :return:
        """
        add_field_payload = {
            "add-field": {
                "name": field_name,
                "type": field_type,
                "stored": False
            }
        }
        result = self._req_session.post(schema_url, data=add_field_payload)