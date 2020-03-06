from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import glob
from pathlib import Path
import logging
import pystache
import subprocess
from . import nfs_mount_parse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
# https://docs.google.com/spreadsheets/d/1CjezKOwkJjk2eyfNTTv_WtkHESe2hzm9c6Ggps6c75E/edit?usp=sharing
SPREADSHEET_ID = '1CjezKOwkJjk2eyfNTTv_WtkHESe2hzm9c6Ggps6c75E'
SHEET_NAME = 'DEV'
RANGE_NAME = 'D2:F'

GRANULE_FILE_ROOT = 'tmp/granule_lists'

CONFIG_TEMPLATE = 'dataset_config_template.yml'
CONFIG_FILE_ROOT = 'tmp/dataset_config'
LOG_FILE_ROOT = 'tmp/logs'

JOB_DEPLOYMENT_TEMPLATE = "/home/loubrieu/deployment-configs/kubernetes/ingest-jobs/job-deployment-template.yml"
CONNECTION_CONFIG = "/home/loubrieu/deployment-configs/kubernetes/ingest-jobs/connection-config.yml"
CONNECTION_PROFILE = "sdap-dev"
NAMESPACE = "nexus-dev"
RUN_JOB_PATH = "/home/loubrieu/deployment-configs/kubernetes/ingest-jobs/"

kubenetes_available = False


def create_granule_list(file_path_pattern, granule_list_file_path, deconstruct_nfs=False):
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
    logger.info("Granule list file created in directory %s", dir_path);
    Path(dir_path).mkdir(parents=True, exist_ok=True)

    if deconstruct_nfs:
        mount_points = nfs_mount_parse.get_nfs_mount_points()

    with open(granule_list_file_path, 'w') as file_handle:
        for list_item in file_list:
            file_path = os.path.join(cwd, list_item)

            if deconstruct_nfs:
                file_path = nfs_mount_parse.replace_mount_point_with_service_path(file_path, mount_points)

            file_handle.write(f'{file_path}\n')


def create_dataset_config(dataset_id, variable_name, target_config_file_path):
    logger.info("Create dataset configuration file %s", target_config_file_path)
    renderer = pystache.Renderer()
    config_content = renderer.render_path(CONFIG_TEMPLATE, {'dataset_id': dataset_id,
                                                            'variable': variable_name})
    logger.info("templated dataset config \n%s", config_content)

    dir_path = os.path.dirname(target_config_file_path)
    logger.info("Dataset configuration file created in directory %s", dir_path);
    Path(dir_path).mkdir(parents=True, exist_ok=True)

    with open(target_config_file_path, "w") as f:
        f.write(config_content)



def collection_row_callback_parse_nfs(row):
    collection_row_callback(row, deconstruct_nfs=True)

def collection_row_callback(row, deconstruct_nfs=False):
    """ Create the configuration launch the ingestion
        for the given collection row
    """
    dataset_id = row[0].strip()
    netcdf_variable = row[1].strip()
    netcdf_file_pattern = row[2].strip()

    granule_list_file_path = os.path.join(GRANULE_FILE_ROOT, f'{dataset_id}-granules.lst')
    create_granule_list(netcdf_file_pattern,
                        granule_list_file_path,
                        deconstruct_nfs = deconstruct_nfs)

    dataset_configuration_file_path = os.path.join(CONFIG_FILE_ROOT, f'{dataset_id}-config.yml')
    create_dataset_config(dataset_id,
                          netcdf_variable,
                          dataset_configuration_file_path)
    cwd = os.getcwd()
    pod_launch_cmd = ['python', '-u', 'runjobs.py',
                      '-flp',  os.path.join(cwd, granule_list_file_path),
                      '-jc',  os.path.join(cwd, dataset_configuration_file_path),
                      '-jg',  dataset_id[:19],                 # the name of container must be less than 63 in total
                      '-jdt', JOB_DEPLOYMENT_TEMPLATE,
                      '-c', CONNECTION_CONFIG,
                      '-p', CONNECTION_PROFILE,
                      'solr', 'cassandra',
                      '-mj', '8',
                      '-nv',  '1.1.0',
                      '-ns', NAMESPACE,
                      '-ds'
                      ]
    logger.info("launch pod with command:\n%s", " ".join(pod_launch_cmd))
    Path(LOG_FILE_ROOT).mkdir(parents=True, exist_ok=True)
    if kubenetes_available:
        with open(os.path.join(cwd, LOG_FILE_ROOT, f'{dataset_id}.out'), 'w') as logfile:
            process = subprocess.Popen(pod_launch_cmd,
                             cwd=RUN_JOB_PATH,
                             stdout=logfile,
                             stderr=logfile)
            process.wait()


def read_google_spreadsheet(tab, row_callback):
    """ Read the given tab in the google spreadsheet
    and apply to each row the callback function"""
    logger.info("Read google spreadsheet %s, tab %s containing collection configurations", SPREADSHEET_ID, tab)
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_console()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    cell_range = f'{tab}!{RANGE_NAME}'
    logger.info("read range %s", cell_range);
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=cell_range).execute()
    values = result.get('values', [])

    if not values:
        logger.info('No data found.')
    else:
        logger.info('Name, Major:')
        for row in values:
            # Print columns A and E, which correspond to indices 0 and 4.
            logger.info('dataset: %s, variable: %s, file path pattern:  %s' % (row[0], row[1], row[2]))
            row_callback(row)


def run_collections():
    """For each collection in the list, creates a granule list file
       Get credential for the google spreadheet api as documented:
       https://console.developers.google.com/apis/credentials
    """
    read_google_spreadsheet(SHEET_NAME, collection_row_callback)


if __name__ == '__main__':
    main()
