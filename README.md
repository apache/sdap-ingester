# SDAP manager for ingestion of datasets

## Prerequisites

### python 3

Install anaconda for python 3. From the graphic install for example for macos:

https://www.anaconda.com/distribution/#macos

### git lfs (for development)

Git lfs for the deployment from git, see https://git-lfs.github.com/

If not available you have to get netcdf files for test, if you do need the tests.

### Deployed nexus on kubernetes cluster

See project https://github.com/apache/incubator-sdap-nexus

    $ helm install nexus .  --namespace=sdap --dependency-update -f ~/overridden-nexus-values.yml 


## Install, Configure and run

### Install

Stay logged in as user

    $ pip install sdap-ingest-manager


### Configure the ingestion system
                            
Catch the message at the end of the installation output

    --------------------------------------------------------------
    Now, create configuration files in
    ***/<some path>>/.sdap_ingest_manager***
     Use templates and examples provided there
    --------------------------------------------------------------

Use the path shown in the message and create your own configuration files:

    $ cd /<some path>>/.sdap_ingest_manager
    $ cp sdap_ingest_manager.ini.default sdap_ingest_manager.ini
    
Edit and update the newly created files by following instructions in the comments.

### Configure the collections

You can configure it in a local yaml file referenced in the `sdap_ingest_manager.ini` file.

It can also be in a google spreadsheet.

If both are configured, the local yaml file will be used.



### Run the ingestion 

On the list of the configured collections:

    $ run_collections

The number of parallel jobs can be updated during the process in the `sdap_ingest_manager.ini` file.

If interrupted (killed) the process will restart where it was.

 
## For developers

### deploy project

    $ bash
    $ git clone ...
    $ cd sdap_ingest_manager
    $ python -m venv venv
    $ source ./venv/bin/activate
    $ pip install .
    
Note the command pip install -e . does not work as it does not deploy the configuration files.

## Update the project

Update the code and the test with your favorite IDE (e.g. pyCharm).

### Test and create the package

Change version in file setup.py 

    $ python setup.py test
    $ git tag <version>
    $ git push origin <version>
    
The release will be automatically pushed to pypi though github action.



