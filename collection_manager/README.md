# SDAP Collection Manager

The SDAP Collection Manager is a service that watches a YAML file (the [Collections
Configuration](#the-collections-configuration-file) file) stored on the filesystem, and all the directories listed in that
file. Whenever new granules are added to any of the watched directories, the Collection
Manager service will publish a message to RabbitMQ to be picked up by the Granule Ingester
(`/granule_ingester` in this repository), which will then ingest the new granules.


## Prerequisites

Python 3.7

Use a conda environment for example:

    $ conda create -n cmenv python=3.7
    $ conda activate cmenv    

## Building the service
From `incubator-sdap-ingester`, run:

    $ cd common && python setup.py install
    $ cd ../collection_manager python setup.py install
    

## Running the service
From `incubator-sdap-ingester`, run:

    $ python collection_manager/collection_manager/main.py -h
    
### The Collections Configuration File

A path to a collections configuration file must be passed in to the Collection Manager
at startup via the `--collections-path` parameter. Below is an example of what the 
collections configuration file could look like:

```yaml
# collections.yaml

collections:

    # The identifier for the dataset as it will appear in NEXUS.
  - id: "CSR-RL06-Mascons_LAND"

    # The path to watch for NetCDF granule files to be associated with this dataset. 
    # This can also be an S3 path prefix, for example "s3://my-bucket/path/to/granules/"
    path: "/data/CSR-RL06-Mascons-land/" 

    # An integer priority level to use when publishing messages to RabbitMQ for historical data. 
    # Higher number = higher priority. Scale is 1-10.
    priority: 1

    # An integer priority level to use when publishing messages to RabbitMQ for forward-processing data.
    # Higher number = higher priority. Scale is 1-10.
    forward-processing-priority: 5 

    # The type of project to use when processing granules in this collection.
    # Accepted values are Grid, ECCO, TimeSeries, or Swath.
    projection: Grid

    dimensionNames:
      # The name of the primary variable
      variable: lwe_thickness

      # The name of the latitude variable
      latitude: lat

      # The name of the longitude variable
      longitude: lon

      # The name of the depth variable (only include if depth variable exists)
      depth: Z 
      
      # The name of the time variable (only include if time variable exists)
      time: Time

    # This section is an index of each dimension on which the primary variable is dependent, mapped to their desired slice sizes.
    slices:
      Z: 1 
      Time: 1
      lat: 60
      lon: 60

 - id: ocean-bottom-pressure 
    path: /data/OBP/
    priority: 6
    forward-processing-priority: 7
    projection: ECCO
    dimensionNames:
      latitude: YC
      longitude: XC
      time: time
      # "tile" is required when using the ECCO projection. This refers to the name of the dimension containing the ECCO tile index.
      tile: tile
      variable: OBP
    slices:
      time: 1
      tile: 1
      i: 30
      j: 30
```

Note that the dimensions listed under `slices` will not necessarily match the values of the properties under `dimensionNames`. This is because sometimes
the actual dimensions are referenced by index variables. 

> **Tip:** An easy way to determine which variables go under `dimensionNames` and which ones go under `slices` is that the variables 
> on which the primary variable is dependent should be listed under `slices`, and the variables on which _those_ variables are dependent 
> (which could be themselves, as in the case of the first collection in the above example) should be the values of the properties under 
> `dimensionNames`. The excepction to this is that `dimensionNames.variable` should always be the name of the primary variable.

## Running the tests
From `incubator-sdap-ingester/`, run:

    $ cd common && python setup.py install
    $ cd ../collection_manager && python setup.py install
    $ pip install pytest && pytest
    
## Building the Docker image
From `incubator-sdap-ingester`, run:

    $ docker build . -f collection_manager/docker/Dockerfile -t nexusjpl/collection-manager
