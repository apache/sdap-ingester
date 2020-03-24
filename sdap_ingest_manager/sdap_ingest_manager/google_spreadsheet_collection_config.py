import os.path
import pickle
import sys
import logging

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        logger.info('Read collection configuration:')
        collections = []
        for row in values:
            logger.info('dataset: %s, variable: %s, file path pattern:  %s, priority: %s' % (row[0], row[1], row[2], row[3]))
            collections.append({'id': row[0].strip(),
                                'variable': row[1].strip(),
                                'path': row[2].strip(),
                                'priority': int(row[3])})
        sorted_collections = sorted(collections, key=lambda c: c['priority'])
        for collection in sorted_collections:
            row_callback(collection)
