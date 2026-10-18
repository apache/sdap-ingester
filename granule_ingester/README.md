# SDAP Granule Ingester

The SDAP Granule Ingester is a service that reads from a RabbitMQ queue for
YAML-formated string messages produced by the Collection Manager (`/collection_manager` 
in this repo). For each message consumed, this service will read a granule file from
disk and ingest it into SDAP by processing the granule and writing the resulting
data to Cassandra and Solr.


## Prerequisites

Python 3.11 and [uv](https://docs.astral.sh/uv/) (uv provisions and manages the
virtual environment).

## Building the service
From `incubator-sdap-ingester`, run:

    $ uv sync --package granule_ingester

This creates `.venv` with the `granule_ingester` package and its dependencies.


## Launching the service
From `incubator-sdap-ingester`, run:

    $ uv run --package granule_ingester python granule_ingester/granule_ingester/main.py -h
    
## Running the tests
From `incubator-sdap-ingester`, run:

    $ uv run --package granule_ingester pytest granule_ingester/tests
    
## Building the Docker image
From `incubator-sdap-ingester`, run:

    $ docker build . -f granule_ingester/docker/Dockerfile -t nexusjpl/granule-ingester
