from __future__ import print_function

from sdap_ingest_manager.history_manager import DatasetIngestionHistory
import glob
import logging
import os.path
import sys
from datetime import datetime
from typing import Dict, List

import pystache
import yaml

from sdap_ingest_manager.util import nfs_mount_parse
from sdap_ingest_manager.publisher import MessagePublisher

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

GROUP_PATTERN = "(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?"
GROUP_DEFAULT_NAME = "group default name"

DEFAULT_DATA_FILE_EXTENSION = ['nc', 'h5']


def collection_row_callback(collection: dict,
                            collection_config_template,
                            granule_file_list_root_path: str,
                            dataset_configuration_root_path: str,
                            history_manager_builder,
                            deconstruct_nfs=False,
                            **pods_run_kwargs
                            ):
    return IngestionOrderExecutor().execute_ingestion_order(collection,
                                                            collection_config_template,
                                                            history_manager_builder,
                                                            deconstruct_nfs,
                                                            **pods_run_kwargs)


class IngestionOrderExecutor:

    def __init__(self):

        # move this out of __init__, pass it in during instantiation
        self._publisher = MessagePublisher(host='localhost', username='guest', password='guest', queue='nexus')
        self._publisher.connect()

    def execute_ingestion_order(self,
                                collection: dict,
                                collection_config_template: str,
                                history_manager: DatasetIngestionHistory,
                                deconstruct_nfs=False,
                                **kwargs):
        """ Create the configuration and launch the ingestion
            for the given collection row
        """
        dataset_id = collection['id']
        path_to_granules = collection['path']
        variable = collection['variable']

        time_range = self._get_time_range(collection)
        granule_list = self._get_granules(path_to_granules, history_manager, deconstruct_nfs, **time_range)
        with open(collection_config_template, 'r') as config_template_file:
            config_template_str = config_template_file.read()
        for granule in granule_list:
            dataset_config = self._fill_template(collection_id=dataset_id,
                                                 variable_name=variable,
                                                 granule=granule,
                                                 config_template=config_template_str)
            self._publisher.publish_message(dataset_config)
            history_manager.push(granule)

    @staticmethod
    def _generate_messages(dataset_config: str, granule_list: List[str]) -> List[str]:
        messages = []
        job_config_dict = yaml.load(dataset_config, yaml.FullLoader)
        for granule_name in granule_list:
            message = {
                "granule": {
                    "resource": granule_name
                },
                **job_config_dict}
            messages.append(yaml.dump(message))
        return messages

    @staticmethod
    def _get_time_range(collection) -> Dict[str, datetime]:
        time_range = {}
        for time_boundary in {"from", "to"}:
            if time_boundary in collection.keys() and collection[time_boundary]:
                # add prefix "from" because is a reserved name which can not be used as function argument
                time_range[f'date_{time_boundary}'] = datetime.fromisoformat(collection[time_boundary])
                logger.info(f"time criteria {time_boundary} is {time_range[f'date_{time_boundary}']}")
        return time_range

    @staticmethod
    def _param_to_str_arg(k, v):
        k_with_dash = k.replace('_', '-')
        str_k = f'--{k_with_dash}'
        if type(v) == bool:
            if v:
                return [str_k]
            else:
                return []
        elif isinstance(v, list):
            return [str_k, ','.join(v)]
        else:
            return [str_k, str(v)]

    @classmethod
    def _get_file_list(cls, file_path_pattern):
        """

        :param file_path_pattern: regular expression or directory which will be extended with default extensions (nc, h5, ...)
        :return: the list of files matching
        """
        logger.info("get files matching %s", file_path_pattern)
        file_path_pattern = os.path.join(sys.prefix, '.sdap_ingest_manager', file_path_pattern)
        logger.info("from sys.prefix directory for relative path %s", file_path_pattern)
        if os.path.isdir(file_path_pattern):
            file_list = []
            for extension in DEFAULT_DATA_FILE_EXTENSION:
                extended_file_path_pattern = os.path.join(file_path_pattern, f'*.{extension}')
                file_list.extend(glob.glob(extended_file_path_pattern))
        else:
            file_list = glob.glob(file_path_pattern)

        logger.info("%i files found", len(file_list))

        return file_list

    @classmethod
    def _get_granules(cls,
                      file_path_pattern: str,
                      history_manager: DatasetIngestionHistory,
                      deconstruct_nfs: bool = False,
                      date_from: datetime = None,
                      date_to: datetime = None):
        """
        Creates a granule list file from a file path pattern
        matching the granules.
        If a granules has already been ingested with same md5sum signature, it is not included in this list.
        When deconstruct_nfs is True, the paths will shown as viewed on the nfs server
        and not as they are mounted on the nfs client where the script runs (default behaviour).
        """

        files_in_directory = cls._get_file_list(file_path_pattern)

        # determine update time range from input option and history_manager
        if history_manager:
            timestamp_to = None
            timestamp_from = history_manager.get_latest_ingested_file_update()
            if timestamp_from is None:
                logger.info("No ingestion history available, forward processing ignored")
        else:
            timestamp_to = date_to.timestamp() if date_to else None
            timestamp_from = date_from.timestamp() if date_from else None

        if deconstruct_nfs:
            mount_points = nfs_mount_parse.get_nfs_mount_points()

        new_files = []
        for file_path in files_in_directory:
            if cls._is_in_time_range(file_path, timestamp_from, timestamp_to):
                filename = os.path.basename(file_path)
                already_ingested = False
                if history_manager:
                    logger.info(f"is file {file_path} already ingested ?")
                    already_ingested = history_manager.has_valid_cache(file_path)
                if not already_ingested:
                    logger.info(f"file {filename} not ingested yet, added to the list")
                    if deconstruct_nfs:
                        file_path = nfs_mount_parse.replace_mount_point_with_service_path(file_path, mount_points)
                    new_files.append(file_path)
                else:
                    logger.debug(f"file {filename} already ingested with same md5sum")
            else:
                logger.debug(f"file {file_path} has not been updated in the targeted time range")
        return new_files

    @staticmethod
    def _fill_template(collection_id: str,
                       variable_name: str,
                       granule: str,
                       config_template: str) -> str:
        renderer = pystache.Renderer()
        config_content = renderer.render(config_template,
                                         {
                                             'granule': granule,
                                             'dataset_id': collection_id,
                                             'variable': variable_name
                                         })
        logger.info(f"Templated dataset config:\n{config_content}")
        return config_content

    @staticmethod
    def _is_in_time_range(file, ts_from, ts_to):
        """
        :param file: file path as a string
        :param ts_from: timestamp, can be None
        :param ts_to: timestamp, can be None
        :return: True is the update time of the file is between ts_from and ts_to. False otherwise
        """
        file_mtimestamp = os.path.getmtime(file)
        status_from = True
        if ts_from:
            if ts_from < file_mtimestamp:
                status_from = True
            else:
                status_from = False

        status_to = True
        if ts_to:
            if ts_to > file_mtimestamp:
                status_to = True
            else:
                status_to = False

        return status_from and status_to
