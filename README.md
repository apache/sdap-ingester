# SDAP manager for ingestion of datasets

## Prerequisites

Install anaconda for python 3. From the graphic install for example for macos:

https://www.anaconda.com/distribution/#macos


## Install

### deploy project

    git clone ...
    cd sdap_ingest_manager
    python -m venv venv
    source ./venv/bin/activate
    pip install -r requirements.txt

Add a credential file "credentials.json" to access reference spreadsheet for dataset configurations:

    
    {
      "installed": {
        "client_id": "<client id>",
        "client_secret": "<client secret>",
        "redirect_uris": [],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://accounts.google.com/o/oauth2/token"
      }
    }
    
Get client id and secret from https://console.developers.google.com/apis/credentials?project=sdap-ingestion-management&folder=&organizationId=
, see section <b>OAuth 2.0 client id</b>

### change configuration

To be detailed

### Test

    python setup.py test


## Run


## For developers

