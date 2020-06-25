# SDAP Collection Manager

The SDAP Collection Manager is a service that watches a YAML file (the Collections
Configuration file) stored on the filesystem, and all the directories listed in that
file. Any time new granules are added to any of the watched directories, the Collection
Manager service will publish a message to RabbitMQ to be picked up by the Granule Ingester
(`/granule_ingester` in this repository), which will then ingest the new granules.


## Prerequisites

Python 3.7

## Building the service
From `incubator-sdap-ingester/collection_manager`, run:

    $ python setup.py install
    

## Running the service
From `incubator-sdap-ingester/collection_manager`, run:

    $ python collection_manager/main.py -h
    
### The Collections Configuration File

A path to a collections configuration file must be passed in to the Collection Manager
on startup via the `--collections-path` parameter. Below is an example of what the 
collections configuration file should look like:

```yaml
# collections.yaml

collections:
  - id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND
    path: /opt/data/grace/*land*.nc
    variable: lwe_thickness
    priority: 1
    forward-processing-priority: 5

  - id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_OCEAN
    path: /opt/data/grace/*ocean*.nc
    variable: lwe_thickness
    priority: 2
    forward-processing-priority: 6

  - id: AVHRR_OI-NCEI-L4-GLOB-v2.0
    path: /opt/data/avhrr/*.nc
    variable: analysed_sst
    priority: 1

```
## Running the tests
From `incubator-sdap-ingester/collection_manager`, run:

    $ pip install pytest
    $ pytest
    
## Building the Docker image
From `incubator-sdap-ingester/collection_manager`, run:

    $ docker build . -f docker/Dockerfile -t nexusjpl/collection-manager
