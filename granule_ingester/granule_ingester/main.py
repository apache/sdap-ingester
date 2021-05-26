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
from granule_ingester.writers import CassandraStore, SolrStore


def cassandra_factory(contact_points, port, username, password):
    store = CassandraStore(contact_points=contact_points, port=port, username=username, password=password)
    store.connect()
    return store


def solr_factory(solr_host_and_port, zk_host_and_port):
    store = SolrStore(zk_url=zk_host_and_port) if zk_host_and_port else SolrStore(solr_url=solr_host_and_port)
    store.connect()
    return store


async def run_health_checks(dependencies: List[HealthCheck]):
    for dependency in dependencies:
        if not await dependency.health_check():
            return False
    return True


async def main(loop):
    parser = argparse.ArgumentParser(description='Listen to RabbitMQ for granule ingestion instructions, and process '
                                                 'and ingest a granule for each message that comes through.')
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
    parser.add_argument('--cassandra-contact-points',
                        default=['localhost'],
                        metavar="HOST",
                        nargs='+',
                        help='List of one or more Cassandra contact points, separated by spaces. (Default: "localhost")')
    parser.add_argument('--cassandra-port',
                        default=9042,
                        metavar="PORT",
                        help='Cassandra port. (Default: 9042)')
    parser.add_argument('--cassandra-username',
                        metavar="USERNAME",
                        default=None,
                        help='Cassandra username. Optional.')
    parser.add_argument('--cassandra-password',
                        metavar="PASSWORD",
                        default=None,
                        help='Cassandra password. Optional.')
    parser.add_argument('--solr-host-and-port',
                        default='http://localhost:8983',
                        metavar='HOST:PORT',
                        help='Solr host and port. (Default: http://localhost:8983)')
    parser.add_argument('--zk_host_and_port',
                        metavar="HOST:PORT")
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
    logging.basicConfig(level=logging_level, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
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
    solr_host_and_port = args.solr_host_and_port
    zk_host_and_port = args.zk_host_and_port

    consumer = MessageConsumer(rabbitmq_host=args.rabbitmq_host,
                               rabbitmq_username=args.rabbitmq_username,
                               rabbitmq_password=args.rabbitmq_password,
                               rabbitmq_queue=args.rabbitmq_queue,
                               data_store_factory=partial(cassandra_factory,
                                                          cassandra_contact_points,
                                                          cassandra_port,
                                                          cassandra_username,
                                                          cassandra_password),
                               metadata_store_factory=partial(solr_factory, solr_host_and_port, zk_host_and_port))
    try:
        solr_store = SolrStore(zk_url=zk_host_and_port) if zk_host_and_port else SolrStore(solr_url=solr_host_and_port)
        await run_health_checks([CassandraStore(cassandra_contact_points,
                                                cassandra_port,
                                                cassandra_username,
                                                cassandra_password),
                                 solr_store,
                                 consumer])
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
