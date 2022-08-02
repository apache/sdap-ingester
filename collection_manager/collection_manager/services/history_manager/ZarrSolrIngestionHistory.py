import hashlib
import logging

import pysolr
import requests
from collection_manager.services.history_manager.IngestionHistory import (IngestionHistory, IngestionHistoryBuilder)
from collection_manager.entities.Collection import Collection
from collections import defaultdict
from common.async_utils.AsyncUtils import run_in_executor
from typing import Awaitable, Callable, Dict, List, Optional, Set

logging.getLogger("pysolr").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def doc_key(dataset_id, file_name):
    return hashlib.sha1(f'{dataset_id}{file_name}'.encode('utf-8')).hexdigest()


class ZarrSolrIngestionHistoryBuilder(IngestionHistoryBuilder):
    def __init__(self, solr_url: str, signature_fun=None):
        self._solr_url = solr_url
        self._signature_fun = signature_fun

    def build(self, dataset_id: str):
        return ZarrSolrIngestionHistory(solr_url=self._solr_url,
                                    dataset_id=dataset_id,
                                    signature_fun=self._signature_fun)


class ZarrSolrIngestionHistory(IngestionHistory):
    # TODO change names for zarrgranules and zarrdatasets
    _granule_collection_name = "zarrgranules"
    _dataset_collection_name = "zarrdatasets"
    _req_session = None

    def __init__(self, collections_path: str, solr_url: str, dataset_id: str, signature_fun=None):
        try:
            self._url_prefix = f"{solr_url.strip('/')}/solr"
            # TODO check method
            self._create_collection_if_needed()
            self.collections_path = collections_path
            # TODO check if this works
            self.collections_by_dir = Dict[collections_path, Set[Collection]] = defaultdict(set)
            self._solr_granules = pysolr.Solr(f"{self._url_prefix}/{self._granule_collection_name}")
            self._solr_datasets = pysolr.Solr(f"{self._url_prefix}/{self._dataset_collection_name}")
            self._dataset_id = dataset_id
            self._signature_fun = signature_fun
            self._latest_ingested_file_update = self._get_latest_file_update()
        except requests.exceptions.RequestException:
            raise DatasetIngestionHistorySolrException(f"solr instance unreachable {solr_url}")

    def __del__(self):
        self._req_session.close()

    @run_in_executor
    def _push_record(self, file_name, signature):   # granule-level JSON entry
        hash_id = doc_key(self._dataset_id, file_name)
        self._solr_granules.delete(q=f"id:{hash_id}")
        self._solr_granules.add([{
            'id': hash_id,
            'dataset_s': self._dataset_id,
            'granule_s': file_name,
            'granule_signature_s': signature}])
        self._solr_granules.commit()
        return None

    @run_in_executor
    def _save_latest_timestamp(self):   # dataset-level JSON entry
        if self._solr_datasets:
            self._solr_datasets.delete(q=f"id:{self._dataset_id}")
            self._solr_datasets.add([{
                'id': self._dataset_id,
                'dataset_s': self._dataset_id,
                'latest_update_l': self._latest_ingested_file_update}])
            self._solr_datasets.commit()
            #{
                #"id": "MUR25-JPL-L4-GLOB-v04.2",
                #"latest_update_l": 1637629358,
                #"_version_": 1718445323844583426,
                #"dataset_s": "MUR25-JPL-L4-GLOB-v04.2",
                #"variables": [{
                    #"name_s": "analysed_sst",
                    #"fill_d": -32768
                #}],
                #"s3_uri_s": "s3://cdms-dev-zarr/MUR25-JPL-L4-GLOB-v04.2/",
                #"public_b": false,
                #"type_s": "gridded",
                #"chunk_shape": [30, 120, 240]
            #}      

    def _get_latest_file_update(self):
        results = self._solr_datasets.search(q=f"id:{self._dataset_id}")
        if results:
            return results.docs[0]['latest_update_l']
        else:
            return None

    @run_in_executor
    def _get_signature(self, file_name):
        hash_id = doc_key(self._dataset_id, file_name)
        results = self._solr_granules.search(q=f"id:{hash_id}")
        if results:
            return results.docs[0]['granule_signature_s']
        else:
            return None

    # TODO check relation and see if need to replace collection path
    def _create_collection_if_needed(self):
        try:
            if not self._req_session:
                self._req_session = requests.session()

            payload = {'action': 'CLUSTERSTATUS'}
            collections_endpoint = f"{self._url_prefix}/admin/collections"
            result = self._req_session.get(collections_endpoint, params=payload)
            response = result.json()
            node_number = len(response['cluster']['live_nodes'])

            existing_collections = response['cluster']['collections'].keys()

            if self._granule_collection_name not in existing_collections:
                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._granule_collection_name,
                           'numShards': node_number
                           }
                result = self._req_session.get(collections_endpoint, params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")

                # Update schema
                schema_endpoint = f"{self._url_prefix}/{self._granule_collection_name}/schema"
                self._add_field(schema_endpoint, "dataset_s", "string")
                self._add_field(schema_endpoint, "granule_s", "string")
                self._add_field(schema_endpoint, "granule_signature_s", "string")

            if self._dataset_collection_name not in existing_collections:
                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._dataset_collection_name,
                           'numShards': node_number
                           }
                result = self._req_session.get(collections_endpoint, params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")

                # Update schema
                schema_endpoint = f"{self._url_prefix}/{self._dataset_collection_name}/schema"
                self._add_field(schema_endpoint, "dataset_s", "string")
                self._add_field(schema_endpoint, "latest_update_l", "TrieLongField")

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
        return self._req_session.post(schema_url, data=str(add_field_payload).encode('utf-8'))


class DatasetIngestionHistorySolrException(Exception):
    pass
