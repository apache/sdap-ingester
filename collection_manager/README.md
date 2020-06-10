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

For development purpose, you might want to expose solr port outside kubernetes

   kubectl port-forward solr-set-0 8983:8983 -n sdap 

 
## For developers

### deploy project

    $ bash
    $ git clone ...
    $ cd sdap_ingest_manager
    $ python -m venv venv
    $ source ./venv/bin/activate
    $ pip install .
    $ pytest -s
    
Note the command pip install -e . does not work as it does not deploy the configuration files.

### Update the project

Update the code and the test with your favorite IDE (e.g. pyCharm).

### Launch for development/tests

### Prerequisite

Deploy a local rabbitmq service, for example with docker.

    docker run -d --hostname localhost -p 5672:5672 --name rabbitmq rabbitmq:3
   
   
### Launch the service


The service reads the collection configuration and submit granule ingestion messages to the message broker (rabbitmq).
For each collection, 2 ingestion priority levels are proposed: the nominal priority, the priority for forward processing (newer files), usually higher. 
An history of the ingested granules is managed so that the ingestion can stop and re-start anytime.

    cd collection_manager
    python main.py -h
    python main.py --collections ../tests/resources/data/collections.yml --history-path=/tmp

# Containerization

TO BE UPDATED

## Docker

    docker build . -f containers/docker/config-operator/Dockerfile --no-cache --tag tloubrieu/sdap-ingest-manager:latest
        
To publish the docker image on dockerhub do (step necessary for kubernetes deployment):

    docker login
    docker push tloubrieu/sdap-ingest-manager:latest
    
## Kubernetes
    
### Launch the service

    kubectl apply -f containers/kubernetes/job.yml -n sdap
    
Delete the service: 

    kubectl delete jobs --all -n sdap
    
    

    

    
    
    
 
    
    





