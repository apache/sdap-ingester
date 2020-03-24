from __future__ import print_function
import sys
import os.path
from pathlib import Path
import re
import glob
import logging
import pystache
import configparser
from . import nfs_mount_parse
import sdap_ingest_manager.kubernetes_ingester
from .util import full_path
from .util import md5sum_from_filepath


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

GROUP_PATTERN = "(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?"
GROUP_DEFAULT_NAME = "group default name"
CONFIG_TEMPLATE = 'dataset_config_template.yml'


def read_local_configuration():
    print("====config====")
    config = configparser.ConfigParser()
    candidates = [full_path('sdap_ingest_manager.ini'),
                  full_path('sdap_ingest_manager.ini.example')]
    config.read(candidates)
    return config


def create_granule_list(file_path_pattern, history_file,
                        granule_list_file_path, deconstruct_nfs=False):
    """ Creates a granule list file from a file path pattern
        matching the granules.
        If a granules has already been ingested with same md5sum signature, it is not included in this list.
        When deconstruct_nfs is True, the paths will shown as viewed on the nfs server
        and not as they are mounted on the nfs client where the script runs (default behaviour).
    """
    logger.info("Create granule list file %s", granule_list_file_path)

    logger.info("using file pattern %s", file_path_pattern)
    logger.info("from sys.prefix directory for relative path %s", os.path.join(sys.prefix, '.sdap_ingest_manager'))
    file_list = glob.glob(os.path.join(sys.prefix, '.sdap_ingest_manager', file_path_pattern))

    logger.info("%i files found", len(file_list))

    dir_path = os.path.dirname(granule_list_file_path)
    logger.info("Granule list file created in directory %s", dir_path)
    Path(dir_path).mkdir(parents=True, exist_ok=True)

    if deconstruct_nfs:
        mount_points = nfs_mount_parse.get_nfs_mount_points()

    logger.info(f"loading history file {history_file}")
    history = {}
    try:
        with open(history_file, 'r') as f_history:
            for line in f_history:
                filename, md5sum = line.strip().split(',')
                history[filename] = md5sum
    except FileNotFoundError:
        logger.info("no history file created yet")

    with open(granule_list_file_path, 'w') as file_handle:
        for file_path in file_list:
            filename = os.path.basename(file_path)
            md5sum = md5sum_from_filepath(file_path)
            if filename not in history.keys() \
                    or (filename in history.keys() and history[filename] != md5sum):
                logger.debug(f"file {filename} not ingested yet, added to the list")
                if deconstruct_nfs:
                    file_path = nfs_mount_parse.replace_mount_point_with_service_path(file_path, mount_points)
                file_handle.write(f'{file_path}\n')
            else:
                logger.debug(f"file {filename} already ingested with same md5sum")

    del history


def create_dataset_config(collection_id, variable_name, collection_config_template, target_config_file_path):
    logger.info("Create dataset configuration file %s", target_config_file_path)
    renderer = pystache.Renderer()
    collection_config_template_path = os.path.join(sys.prefix, collection_config_template)
    config_content = renderer.render_path(collection_config_template_path, {'dataset_id': collection_id,
                                                            'variable': variable_name})
    logger.info("templated dataset config \n%s", config_content)

    dir_path = os.path.dirname(target_config_file_path)
    logger.info("Dataset configuration file created in directory %s", dir_path)
    Path(dir_path).mkdir(parents=True, exist_ok=True)

    with open(target_config_file_path, "w") as f:
        f.write(config_content)


def collection_row_callback(row,
                            collection_config_template,
                            granule_file_list_root_path,
                            dataset_configuration_root_path,
                            history_root_path,
                            deconstruct_nfs=False,
                            **pods_run_kwargs
                            ):
    """ Create the configuration and launch the ingestion
        for the given collection row
    """
    dataset_id = row[0].strip()
    netcdf_variable = row[1].strip()
    netcdf_file_pattern = row[2].strip()

    granule_list_file_path = os.path.join(granule_file_list_root_path,
                                          f'{dataset_id}-granules.lst')
    history_file_path = os.path.join(history_root_path,
                                     f'{dataset_id}.csv')
    create_granule_list(netcdf_file_pattern,
                        history_file_path,
                        granule_list_file_path,
                        deconstruct_nfs=deconstruct_nfs)

    dataset_configuration_file_path = os.path.join(dataset_configuration_root_path,
                                                   f'{dataset_id}-config.yml')

    create_dataset_config(dataset_id,
                          netcdf_variable,
                          collection_config_template,
                          dataset_configuration_file_path)
    cwd = os.getcwd()
    # group must match the following regex (([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?
    # and when suffixed with uid must not exceed 64 characters
    prog = re.compile(GROUP_PATTERN)
    group = prog.match(dataset_id[:19])
    if group is None:
        group_name = GROUP_DEFAULT_NAME
    else:
        group_name = group.group(0)
    pods_run_kwargs['file_list_path'] = granule_list_file_path
    pods_run_kwargs['job_config'] = dataset_configuration_file_path
    pods_run_kwargs['job_group'] = group_name
    pods_run_kwargs['ningester_version'] = '1.1.0'
    pods_run_kwargs['delete_successful'] = True
    pods_run_kwargs['history_file'] = os.path.join(history_root_path, f'{dataset_id}.csv')

    def param_to_str_arg(k, v):
        str_k = f'--{k}'
        if type(v) == bool:
            if v:
                return [str_k]
            else:
                return []
        else:
            return [str_k, str(v)]

    pod_launch_options = [param_to_str_arg(k, v) for (k,v) in pods_run_kwargs.items()]
    flat_pod_launch_options = [item for option in pod_launch_options for item in option]
    pod_launch_cmd = ['run_granule'] + flat_pod_launch_options
    logger.info("launch pod with command:\n%s", " ".join(pod_launch_cmd))
    sdap_ingest_manager.kubernetes_ingester.create_and_run_jobs(**pods_run_kwargs)


