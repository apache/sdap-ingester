# SDAP Collection Manager

The SDAP Collection Manager is a service that watches a YAML file (the [Collections
Configuration](#the-collections-configuration-file) file) stored on the filesystem, and all the directories listed in that
file. Whenever new granules are added to any of the watched directories, the Collection
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
at startup via the `--collections-path` parameter. Below is an example of what the 
collections configuration file should look like:

```yaml
# collections.yaml

collections:

    # The identifier for the dataset as it will appear in NEXUS.
  - id: TELLUS_GRACE_MASCON_CRI_GRID_RL05_V2_LAND 

    # The local path to watch for NetCDF granule files to be associated with this dataset. 
    # Supports glob-style patterns.
    path: /opt/data/grace/*land*.nc 

    # The name of the NetCDF variable to read when ingesting granules into NEXUS for this dataset.
    variable: lwe_thickness 

    # An integer priority level to use when publishing messages to RabbitMQ for historical data. 
    # Higher number = higher priority.
    priority: 1 

    # An integer priority level to use when publishing messages to RabbitMQ for forward-processing data.
    # Higher number = higher priority.
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
From `incubator-sdap-ingester/`, run:

    $ cd common && python setup.py install
    $ cd ../collection_manager && python setup.py install
    $ pip install pytest && pytest
    
## Building the Docker image
From `incubator-sdap-ingester/collection_manager`, run:

    $ docker build . -f docker/Dockerfile -t nexusjpl/collection-manager
