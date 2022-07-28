# SDAP Granule Ingester

The SDAP Granule Ingester is a service that reads from a RabbitMQ queue for
YAML-formated string messages produced by the Collection Manager (`/collection_manager` 
in this repo). For each message consumed, this service will read a granule file from
disk and ingest it into SDAP by processing the granule and writing the resulting
data to Cassandra and Solr.


## Prerequisites

Python 3.7 (```conda install -c anaconda python=3.7``` in conda env)

## Building the service
From `incubator-sdap-ingester`, run:

    cd common && python setup.py install
    cd ../granule_ingester && python setup.py install

# Install nexusproto

Clone repo

    git clone https://github.com/apache/incubator-sdap-nexusproto.git

From `incubator-sdap-nexus-proto`, run:

    cd build/python/nexusproto && python setup.py install
    
## Launching the service
From `incubator-sdap-ingester`, run:

    python granule_ingester/granule_ingester/main.py -h

In order to successfully run the service, you will need to have a Cassandra, 
Solr, and RabbitMQ connection. Make sure to provide their respective credentials.

### How to Launch Service with Port-Forwarded Instances in `bigdata`

**NOTE: Run each of the following in different terminals**

#### Cassandra
Connect to bigdata
    
    ssh -L 9042:localhost:9042 bigdata

Log in and then run the following

    ssh sdeploy@bigdata

Now, port forward the service

    kubectl port-forward svc/nexus-cassandra -n sdap 9042:9042
    
#### Solr
Connect to bigdata
    
    ssh -L 8983:localhost:8984 bigdata

Log in and then run the following

    ssh sdeploy@bigdata

Now, port forward the service

    kubectl port-forward svc/nexus-solr-svc -n sdap 8984:8983
    
#### RabbitMQ
Connect to bigdata
    
    ssh -L 5672:localhost:5672 bigdata

Log in and then run the following

    ssh sdeploy@bigdata

Now, port forward the service

    kubectl port-forward svc/rabbitmq -n sdap 5672:5672
    
From `incubator-sdap-ingester`, run:

    python granule_ingester/granule_ingester/main.py --cassandra-username=cassandra --cassandra-password=cassandra
    
## Running the tests
From `incubator-sdap-ingester`, run:

    $ cd common && python setup.py install
    $ cd ../granule_ingester && python setup.py install
    $ pip install pytest && pytest
    
## Building the Docker image
From `incubator-sdap-ingester`, run:

    $ docker build . -f granule_ingester/docker/Dockerfile -t nexusjpl/granule-ingester
