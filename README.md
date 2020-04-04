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

If the path does not show in the installation stdout, you can find it with the command:

    python -c "import sys; print(f'{sys.prefix}/.sdap_ingest_manager')"


Use the path shown in the message and create your own configuration files:

    $ cd /<some path>>/.sdap_ingest_manager
    $ cp sdap_ingest_manager.ini.default sdap_ingest_manager.ini
    
Edit and update the newly created files by following instructions in the comments.

Note that the `.ini.default` file will be used if no value is configured in the `.ini` file. So you can have a simplified `.ini` file with only your specific configuration.
Don't put your specific configuration in the `.ini.default` file, it will be replaced when you upgrade the package.

Example of a simplified `.ini` file:

```yaml
[COLLECTIONS_YAML_CONFIG]
yaml_file = collections.yml

[OPTIONS]
# set to False to actually call the ingestion command for each granule
dry_run = False
# set to True to automatically list the granules as seen on the nfs server when they are mounted on the local file system.
deconstruct_nfs = True
# number of parallel ingestion pods on kubernetes (1 per granule)
parallel_pods = 2

[INGEST]
# kubernetes namespace where the sdap cluster is deployed
kubernetes_namespace = nexus-dev

```


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

A package based on the dev branch is automatically published at github release when a push is made. 


Change version in file setup.py 

    $ python setup.py test
    $ git tag <version>
    $ git push origin <version>
    
The release will be automatically pushed to pypi though github action.



# Docker deployment

(development version)

    cd docker
    docker build --tag nexusjpl/collection-ingester:latest .    
    docker run -it --name collection_ingester -v sdap_ingest_config:/usr/local/.sdap_ingest_manager nexusjpl/collection-ingester:latest
    docker volume inspect sdap_ingest_config



