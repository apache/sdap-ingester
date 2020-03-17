# SDAP manager for ingestion of datasets

## Prerequisites

### python 3

Install anaconda for python 3. From the graphic install for example for macos:

https://www.anaconda.com/distribution/#macos

### git lfs (for development)

Git lfs for the deployment from git, see https://git-lfs.github.com/

If not available you have to get netcdf files for test, if you do need the tests.


## Install, Configure and run

Install

    Stay logged in a user

    $ pip install -v https://github.com/tloubrieu-jpl/incubator-sdap-nexus-ingestion-manager/releases/download/0.0.1/sdap_ingest_manager-0.0.1.tar.gz
                            
Catch the message at the end of the installation output

    --------------------------------------------------------------
    Now, create configuration files in
    ***/<some path>>/.sdap_ingest_manager***
     Use templates and examples provided there
    --------------------------------------------------------------

Use the path shown in the message and create your own configuration files:

    $ cd /<some path>>/.sdap_ingest_manager
    $ cp credentials.json.template credentials.json
    $ cp sdap_ingest_manager.ini.example sdap_ingest_manager.ini
    
Edit and update the newly created files by following instructions in the comments.

Run the ingestion on your list of collections:

    $ run_collections


## For developers

### deploy project

    $ bash
    $ git clone ...
    $ cd sdap_ingest_manager
    $ python -m venv venv
    $ source ./venv/bin/activate
    $ pip install .

## Update the project

Update the code and the test with your favorite IDE (e.g. pyCharm).

### Test and create the package

    $ python setup.py test
    $ git tag <version>
    $ git push origin <version>
    $ python setup.py sdist bdist_wheel

Create a tag and publish the package as a gitHub release.





