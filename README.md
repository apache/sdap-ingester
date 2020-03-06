# SDAP manager for ingestion of datasets

## Prerequisites

### python 3
Install anaconda for python 3. From the graphic install for example for macos:

https://www.anaconda.com/distribution/#macos

### git lfs
Git lfs for the deployment from git

If not available you have to get netcdf file for test, if you do need the tests.


## Install

### deploy project

    bash
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

### Test and deploy

    python setup.py test
    python setup.py install
    pip install .

## Run

    run_collectrions


## For developers

