# SDAP Granule Ingester

The SDAP Granule Ingester is a service that reads from a RabbitMQ queue for
YAML-formated string messages produced by the Collection Manager (`/collection_manager` 
in this repo). For each message consumed, this service will read a granule file from
disk and ingest it into SDAP by processing the granule and writing the resulting
data to Cassandra and Solr.


## Prerequisites

Python 3.7, conda

Create a virtual environment:

    conda create -n nexus-ingester python=3.7

Activate it:

    conda activate nexus-ingester

## Building the service
From `incubator-sdap-ingester`, run:
    $ cd common && python setup.py install
    $ cd ../granule_ingester && python setup.py install
    

## Launching the service
From `incubator-sdap-ingester`, run:

    $ python granule_ingester/granule_ingester/main.py -h
    
## Running the tests
From `incubator-sdap-ingester`, run:

    $ cd common && python setup.py install
    $ cd ../granule_ingester && python setup.py install
    $ pip install pytest && pytest
    
## Building the Docker image
From `incubator-sdap-ingester`, run:

    $ docker build . -f granule_ingester/docker/Dockerfile -t nexusjpl/granule-ingester
