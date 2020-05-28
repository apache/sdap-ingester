import hashlib
import logging

import pysolr
import requests

from sdap_ingest_manager.history_manager import IngestionHistory, md5sum_from_filepath

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def doc_key(dataset_id, file_name):
    return hashlib.sha1(f'{dataset_id}{file_name}'.encode('utf-8')).hexdigest()


class SolrIngestionHistory(IngestionHistory):
    _granule_collection_name = "nexusgranules"
    _dataset_collection_name = "nexusdatasets"
    _req_session = None

    def __init__(self, solr_url, dataset_id, signature_fun=None):
        try:
            self._solr_url = solr_url
            self._create_collection_if_needed()
            self._solr_granules = pysolr.Solr('/'.join([solr_url.strip('/'), self._granule_collection_name]))
            self._solr_datasets = pysolr.Solr('/'.join([solr_url.strip('/'), self._dataset_collection_name]))
            self._dataset_id = dataset_id
            self._signature_fun = md5sum_from_filepath if signature_fun is None else signature_fun
            self._latest_ingested_file_update = self._get_latest_file_update()
        except requests.exceptions.RequestException:
            raise DatasetIngestionHistorySolrException(f"solr instance unreachable {solr_url}")

    def __del__(self):
        self._push_latest_ingested_date()
        self._req_session.close()

    def _push_record(self, file_name, signature):
        hash_id = doc_key(self._dataset_id, file_name)
        self._solr_granules.delete(q=f"id:{hash_id}")
        self._solr_granules.add([{
            'id': hash_id,
            'dataset_s': self._dataset_id,
            'granule_s': file_name,
            'granule_signature_s': signature}])
        self._solr_granules.commit()
        return None

    def _push_latest_ingested_date(self):
        if self._solr_datasets:
            self._solr_datasets.delete(q=f"id:{self._dataset_id}")
            self._solr_datasets.add([{
                'id': self._dataset_id,
                'dataset_s': self._dataset_id,
                'latest_update_l': self._latest_ingested_file_update}])
            self._solr_datasets.commit()

    def _get_latest_file_update(self):
        results = self._solr_datasets.search(q=f"id:{self._dataset_id}")
        if results:
            return results.docs[0]['latest_update_l']
        else:
            return None

    def _get_signature(self, file_name):
        hash_id = doc_key(self._dataset_id, file_name)
        results = self._solr_granules.search(q=f"id:{hash_id}")
        if results:
            return results.docs[0]['granule_signature_s']
        else:
            return None

    def _create_collection_if_needed(self):
        try:
            if not self._req_session:
                self._req_session = requests.session()

            payload = {'action': 'CLUSTERSTATUS'}
            result = self._req_session.get('/'.join([self._solr_url.strip('/'), 'admin', 'collections']),
                                           params=payload)
            response = result.json()
            node_number = len(response['cluster']['live_nodes'])

            existing_collections = response['cluster']['collections'].keys()

            if self._granule_collection_name not in existing_collections:
                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._granule_collection_name,
                           'numShards': node_number
                           }
                result = self._req_session.get('/'.join([self._solr_url.strip("/"), 'admin', 'collections']),
                                               params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")
                # Update schema
                schema_url = '/'.join([self._solr_url.strip('/'), self._granule_collection_name, 'schema'])
                # granule_s # dataset_s so that all the granule of a dataset are less likely to be on the same shard
                # self.add_unique_key_field(schema_url, "uniqueKey_s", "StrField")
                self._add_field(schema_url, "dataset_s", "StrField")
                self._add_field(schema_url, "granule_s", "StrField")
                self._add_field(schema_url, "granule_signature_s", "StrField")

            else:
                logger.info(f"collection {self._granule_collection_name} already exists")

            if self._dataset_collection_name not in existing_collections:
                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._dataset_collection_name,
                           'numShards': node_number
                           }
                result = self._req_session.get('/'.join([self._solr_url.strip('/'), 'admin', 'collections']),
                                               params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")
                # Update schema
                # http://localhost:8983/solr/nexusdatasets/schema?_=1588555874864&wt=json
                schema_url = '/'.join([self._solr_url.strip('/'), self._dataset_collection_name, 'schema'])
                # self.add_unique_key_field(schema_url, "uniqueKey_s", "StrField")
                self._add_field(schema_url, "dataset_s", "StrField")
                self._add_field(schema_url, "latest_update_l", "TrieLongField")

            else:
                logger.info(f"collection {self._dataset_collection_name} already exists")

        except requests.exceptions.RequestException as e:
            logger.error(f"solr instance unreachable {self._solr_url}")
            raise e

    def _add_field(self, schema_url, field_name, field_type):
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
        result = self._req_session.post(schema_url, data=add_field_payload.__str__())


class DatasetIngestionHistorySolrException(Exception):
    pass
