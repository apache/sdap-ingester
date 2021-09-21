# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import asyncio
import logging
import sys
from functools import partial
from typing import List

from granule_ingester.consumer import MessageConsumer
from granule_ingester.exceptions import FailedHealthCheckError, LostConnectionError
from granule_ingester.healthcheck import HealthCheck
from granule_ingester.writers import CassandraStore, SolrStore, S3ObjectStore
from granule_ingester.writers.ElasticsearchStore import ElasticsearchStore


def obj_store_factory(bucket, region):
    return S3ObjectStore(bucket, region)


def cassandra_factory(contact_points, port, keyspace, username, password):
    store = CassandraStore(contact_points=contact_points, port=port, keyspace=keyspace, username=username, password=password)
    store.connect()
    return store


def solr_factory(solr_host_and_port, zk_host_and_port):
    store = SolrStore(zk_url=zk_host_and_port) if zk_host_and_port else SolrStore(solr_url=solr_host_and_port)
    store.connect()
    return store


def elasticsearch_factory(elastic_url, username, password, index):
    store = ElasticsearchStore(elastic_url, username, password, index)
    store.connect()
    return store


async def run_health_checks(dependencies: List[HealthCheck]):
    for dependency in dependencies:
        if not await dependency.health_check():
            return False
    return True


VALID_DATA_STORE = ['OBJECT_STORE', 'CASSANDRA']

async def main(loop):
    parser = argparse.ArgumentParser(description='Listen to RabbitMQ for granule ingestion instructions, and process '
                                                 'and ingest a granule for each message that comes through.')
    # RABBITMQ
    parser.add_argument('--rabbitmq-host',
                        default='localhost',
                        metavar='HOST',
                        help='RabbitMQ hostname to connect to. (Default: "localhost")')
    parser.add_argument('--rabbitmq-username',
                        default='guest',
                        metavar='USERNAME',
                        help='RabbitMQ username. (Default: "guest")')
    parser.add_argument('--rabbitmq-password',
                        default='guest',
                        metavar='PASSWORD',
                        help='RabbitMQ password. (Default: "guest")')
    parser.add_argument('--rabbitmq-queue',
                        default="nexus",
                        metavar="QUEUE",
                        help='Name of the RabbitMQ queue to consume from. (Default: "nexus")')

    # DATA STORE
    parser.add_argument('--data-store',
                        metavar='DATA_STORE',
                        required=True,
                        help=f'Which data store to use. {VALID_DATA_STORE}')
    # CASSANDRA
    parser.add_argument('--cassandra-contact-points',
                        default=['localhost'],
                        metavar="HOST",
                        nargs='+',
                        help='List of one or more Cassandra contact points, separated by spaces. (Default: "localhost")')
    parser.add_argument('--cassandra-port',
                        default=9042,
                        metavar="PORT",
                        help='Cassandra port. (Default: 9042)')
    parser.add_argument('--cassandra-keyspace', 
                        default='nexustiles',
                        metavar='KEYSPACE',
                        help='Cassandra Keyspace (Default: "nexustiles")')
    parser.add_argument('--cassandra-username',
                        metavar="USERNAME",
                        default=None,
                        help='Cassandra username. Optional.')
    parser.add_argument('--cassandra-password',
                        metavar="PASSWORD",
                        default=None,
                        help='Cassandra password. Optional.')
    #OBJECT STORE
    parser.add_argument('--object-store-bucket',
                        metavar="OBJECT-STORE-BUCKET",
                        default=None,
                        help='OBJECT-STORE-BUCKET. Required if OBJECT_STORE is used')
    parser.add_argument('--object-store-region',
                        metavar="OBJECT-STORE-REGION",
                        default=None,
                        help='OBJECT-STORE-REGION. Required if OBJECT_STORE is used.')
    # METADATA STORE
    parser.add_argument('--metadata-store',
                        default='solr',
                        metavar='STORE',
                        help='Which metadata store to use')

    # SOLR + ZK
    parser.add_argument('--solr-host-and-port',
                        default='http://localhost:8983',
                        metavar='HOST:PORT',
                        help='Solr host and port. (Default: http://localhost:8983)')
    parser.add_argument('--zk_host_and_port',
                        metavar="HOST:PORT")
    
    # ELASTIC
    parser.add_argument('--elastic-url', 
                        default='http://localhost:9200', 
                        metavar='ELASTIC_URL', 
                        help='ElasticSearch URL:PORT (Default: http://localhost:9200)')
    parser.add_argument('--elastic-username', 
                        metavar='ELASTIC_USER', 
                        help='ElasticSearch username')
    parser.add_argument('--elastic-password', 
                        metavar='ELASTIC_PWD', 
                        help='ElasticSearch password')
    parser.add_argument('--elastic-index', 
                        default='nexustiles', 
                        metavar='ELASTIC_INDEX', 
                        help='ElasticSearch index')
    
    # OTHERS
    parser.add_argument('--max-threads',
                        default=16,
                        metavar='MAX_THREADS',
                        help='Maximum number of threads to use when processing granules. (Default: 16)')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        help='Print verbose logs.')

    args = parser.parse_args()

    logging_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=logging_level)
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        logger.setLevel(logging_level)

    logger = logging.getLogger(__name__)

    config_values_str = "\n".join(["{} = {}".format(arg, getattr(args, arg)) for arg in vars(args)])
    logger.info("Using configuration values:\n{}".format(config_values_str))
    cassandra_username = args.cassandra_username
    cassandra_password = args.cassandra_password
    cassandra_contact_points = args.cassandra_contact_points
    cassandra_port = args.cassandra_port
    cassandra_keyspace = args.cassandra_keyspace
    obj_store_bucket = args.object_store_bucket
    obj_store_region = args.object_store_region

    data_store = args.data_store.upper()
    if data_store not in VALID_DATA_STORE:
        logger.error(f'invalid data store: {data_store} vs. {VALID_DATA_STORE}')
        sys.exit(1)
    metadata_store = args.metadata_store    

    solr_host_and_port = args.solr_host_and_port
    zk_host_and_port = args.zk_host_and_port

    elastic_url = args.elastic_url
    elastic_username = args.elastic_username
    elastic_password = args.elastic_password
    elastic_index = args.elastic_index

    msg_consumer_params = {
        'rabbitmq_host': args.rabbitmq_host,
        'rabbitmq_username': args.rabbitmq_username,
        'rabbitmq_password': args.rabbitmq_password,
        'rabbitmq_queue': args.rabbitmq_queue,
    }
    if metadata_store == 'solr':
        metadata_store_obj = SolrStore(zk_url=zk_host_and_port) if zk_host_and_port else SolrStore(solr_url=solr_host_and_port)
        msg_consumer_params['metadata_store_factory'] = partial(solr_factory, solr_host_and_port, zk_host_and_port)
    else:
        metadata_store_obj = ElasticsearchStore(elastic_url, elastic_username, elastic_password, elastic_index)
        msg_consumer_params['metadata_store_factory'] = partial(elasticsearch_factory,
                                                                elastic_url,
                                                                elastic_username,
                                                                elastic_password,
                                                                elastic_index)
    if data_store == 'CASSANDRA':
        msg_consumer_params['data_store_factory'] = partial(cassandra_factory,
                                                            cassandra_contact_points,
                                                            cassandra_port,
                                                            cassandra_keyspace,
                                                            cassandra_username,
                                                            cassandra_password)
        data_store_obj = CassandraStore(cassandra_contact_points,
                                        cassandra_port,
                                        cassandra_keyspace,
                                        cassandra_username,
                                        cassandra_password)
    elif data_store == 'OBJECT_STORE':
        msg_consumer_params['data_store_factory'] = partial(obj_store_factory, obj_store_bucket, obj_store_region)
        data_store_obj = S3ObjectStore(obj_store_bucket, obj_store_region)
    else:
        logger.error(f'invalid data_store: {data_store} vs. {VALID_DATA_STORE}')
        sys.exit(1)
    consumer = MessageConsumer(**msg_consumer_params)
    try:
        await run_health_checks([data_store_obj, metadata_store_obj, consumer])
        async with consumer:
            logger.info("All external dependencies have passed the health checks. Now listening to message queue.")
            await consumer.start_consuming(args.max_threads)
    except FailedHealthCheckError as e:
        logger.error(f"Quitting because not all dependencies passed the health checks: {e}")
    except LostConnectionError as e:
        logger.error(f"{e} Any messages that were being processed have been re-queued. Quitting.")
    except Exception as e:
        logger.exception(f"Shutting down because of an unrecoverable error:\n{e}")
    finally:
        sys.exit(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
