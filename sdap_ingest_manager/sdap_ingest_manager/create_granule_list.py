from __future__ import print_function
import pickle
import sys
import site
import os.path
from pathlib import Path
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import glob

import logging
import pystache
import subprocess
import configparser
from . import nfs_mount_parse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GROUP_PATTERN = "(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?"
GROUP_DEFAULT_NAME = "group default name"
CONFIG_TEMPLATE = 'dataset_config_template.yml'


def read_local_configuration():
    print("====config====")
    config = configparser.ConfigParser()
    candidates = [os.path.join(sys.prefix, '.sdap_ingest_manager/sdap_ingest_manager.ini'),
                  'sdap_ingest_manager.ini',
                  'sdap_ingest_manager/sdap_ingest_manager/resources/config/sdap_ingest_manager.ini',
                  'sdap_ingest_manager/sdap_ingest_manager/resources/config/sdap_ingest_manager.ini.example']
    config.read(candidates)
    return config


def create_granule_list(file_path_pattern,
                        granule_list_file_path, deconstruct_nfs=False):
    """ Creates a granule list file from a file path pattern
        matching the granules.
        When deconstruct_nfs is True, the paths will shown as viewed on the nfs server
        and not as they are mounted on the nfs client where the script runs (default behaviour).
    """
    logger.info("Create granule list file %s", granule_list_file_path)

    logger.info("using file pattern %s", file_path_pattern)
    cwd = os.getcwd()
    logger.info("from current work directory %s", cwd)
    file_list = glob.glob(file_path_pattern)

    logger.info("%i files found", len(file_list))

    dir_path = os.path.dirname(granule_list_file_path)
    logger.info("Granule list file created in directory %s", dir_path)
    Path(dir_path).mkdir(parents=True, exist_ok=True)

    if deconstruct_nfs:
        mount_points = nfs_mount_parse.get_nfs_mount_points()

    with open(granule_list_file_path, 'w') as file_handle:
        for list_item in file_list:
            file_path = os.path.join(cwd, list_item)

            if deconstruct_nfs:
                file_path = nfs_mount_parse.replace_mount_point_with_service_path(file_path, mount_points)

            file_handle.write(f'{file_path}\n')


def create_dataset_config(dataset_id, variable_name, collection_config_template, target_config_file_path):
    logger.info("Create dataset configuration file %s", target_config_file_path)
    renderer = pystache.Renderer()
    collection_config_template_path = os.path.join(sys.prefix, collection_config_template)
    config_content = renderer.render_path(collection_config_template_path, {'dataset_id': dataset_id,
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
                            ingestion_log_root_path,
                            job_deployment_template,
                            connection_config,
                            connection_profile,
                            kubernetes_namespace,
                            parallel_pods,
                            deconstruct_nfs=False,
                            dry_run=True,
                            ):
    """ Create the configuration and launch the ingestion
        for the given collection row
    """
    dataset_id = row[0].strip()
    netcdf_variable = row[1].strip()
    netcdf_file_pattern = row[2].strip()

    granule_list_file_path = os.path.join(granule_file_list_root_path, f'{dataset_id}-granules.lst')
    create_granule_list(netcdf_file_pattern,
                        granule_list_file_path,
                        deconstruct_nfs=deconstruct_nfs)

    dataset_configuration_file_path = os.path.join(dataset_configuration_root_path, f'{dataset_id}-config.yml')
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
    pod_launch_cmd = ['run_granule',
                      '-flp', os.path.join(cwd, granule_list_file_path),
                      '-jc', os.path.join(cwd, dataset_configuration_file_path),
                      '-jg', group_name,  # the name of container must be less than 63 in total
                      '-jdt', os.path.join(sys.prefix, job_deployment_template),
                      '-c', os.path.join(sys.prefix, connection_config),
                      '-p', connection_profile,
                      'solr', 'cassandra',
                      '-mj', parallel_pods,
                      '-nv', '1.1.0',
                      '-ns', kubernetes_namespace,
                      '-ds'
                      ]
    logger.info("launch pod with command:\n%s", " ".join(pod_launch_cmd))
    Path(ingestion_log_root_path).mkdir(parents=True, exist_ok=True)
    if not dry_run:
        with open(os.path.join(cwd, ingestion_log_root_path, f'{dataset_id}.out'), 'w') as logfile:
            process = subprocess.Popen(pod_launch_cmd,
                                       stdout=logfile,
                                       stderr=logfile)
            process.wait()


def read_google_spreadsheet(scope, spreadsheet_id, tab, cell_range, row_callback):
    """ Read the given tab in the google spreadsheet
    and apply to each row the callback function.
    Get credential for the google spreadheet api as documented:
    https://console.developers.google.com/apis/credentials
    """
    logger.info("Read google spreadsheet %s, tab %s containing collection configurations",
                spreadsheet_id,
                tab)
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    token_file_path = os.path.join(sys.prefix, '.sdap_ingest_manager', 'token.pickle')
    if os.path.exists(token_file_path):
        with open(token_file_path, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            scopes = [scope]
            logger.info("scopes %s", scopes)
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(sys.prefix, '.sdap_ingest_manager', 'credentials.json'),
                scopes)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open(token_file_path, 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    tab_cell_range = f'{tab}!{cell_range}'
    logger.info("read range %s", tab_cell_range)
    result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                range=tab_cell_range).execute()
    values = result.get('values', [])

    if not values:
        logger.info('No data found.')
    else:
        logger.info('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            logger.info('dataset: %s, variable: %s, file path pattern:  %s' % (row[0], row[1], row[2]))
            row_callback(row)


