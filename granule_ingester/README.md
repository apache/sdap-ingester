# SDAP Granule Ingester

The SDAP Granule Ingester is a service that reads from a RabbitMQ queue for
YAML-formated string messages produced by the Collection Manager (`/collection_manager` 
in this repo). For each message consumed, this service will read a granule file from
disk and ingest it into SDAP by processing the granule and writing the resulting
data to Cassandra and Solr.


## Prerequisites

Python 3.7

## Building the service
From `incubator-sdap-ingester/granule_ingester`, run:

    $ python setup.py install
    

## Launching the service
From `incubator-sdap-ingester/granule_ingester`, run:

    $ python granule_ingester/main.py -h
    
## Running the tests
From `incubator-sdap-ingester/granule_ingester`, run:

    $ pip install pytest
    $ pytest
    
## Building the Docker image
From `incubator-sdap-ingester/granule_ingester`, run:

    $ docker build . -f docker/Dockerfile -t nexusjpl/granule-ingester
