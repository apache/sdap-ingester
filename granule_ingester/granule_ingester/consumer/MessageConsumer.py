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

import logging

import aio_pika
from granule_ingester.exceptions import PipelineBuildingError, PipelineRunningError, RabbitMQLostConnectionError, \
    RabbitMQFailedHealthCheckError, LostConnectionError
from granule_ingester.healthcheck import HealthCheck
from granule_ingester.pipeline import Pipeline

logger = logging.getLogger(__name__)


class MessageConsumer(HealthCheck):

    def __init__(self,
                 rabbitmq_host,
                 rabbitmq_username,
                 rabbitmq_password,
                 rabbitmq_queue,
                 data_store_factory,
                 metadata_store_factory,
                 log_level=logging.INFO):
        self._rabbitmq_queue = rabbitmq_queue
        self._data_store_factory = data_store_factory
        self._metadata_store_factory = metadata_store_factory

        self._connection_string = "amqp://{username}:{password}@{host}/".format(username=rabbitmq_username,
                                                                                password=rabbitmq_password,
                                                                                host=rabbitmq_host)
        self._connection: aio_pika.Connection = None
        self._level = log_level

    async def health_check(self) -> bool:
        try:
            connection = await self._get_connection()
            await connection.close()
            return True
        except Exception:
            raise RabbitMQFailedHealthCheckError(f"Cannot connect to RabbitMQ! "
                                                 f"Connection string was {self._connection_string}")

    async def _get_connection(self) -> aio_pika.Connection:
        return await aio_pika.connect_robust(self._connection_string)

    async def __aenter__(self):
        self._connection = await self._get_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            await self._connection.close()

    @staticmethod
    async def _received_message(message: aio_pika.IncomingMessage,
                                data_store_factory,
                                metadata_store_factory,
                                pipeline_max_concurrency: int,
                                log_level=logging.INFO):
        logger.info("Received a job from the queue. Starting pipeline.")
        try:
            config_str = message.body.decode("utf-8")
            logger.debug(config_str)
            pipeline = Pipeline.from_string(config_str=config_str,
                                            data_store_factory=data_store_factory,
                                            metadata_store_factory=metadata_store_factory,
                                            max_concurrency=pipeline_max_concurrency)
            pipeline.set_log_level(log_level)
            await pipeline.run()
            await message.ack()
        except PipelineBuildingError as e:
            await message.reject()
            logger.exception(f"Failed to build the granule-processing pipeline. This message will be dropped "
                             f"from RabbitMQ. The exception was:\n{e}")
        except PipelineRunningError as e:
            await message.reject()
            logger.exception(f"Processing the granule failed. It will not be retried. The exception was:\n{e}")
        except LostConnectionError:
            # Let main() handle this
            raise
        except Exception as e:
            await message.reject(requeue=True)
            logger.exception(f"Processing message failed. Message will be re-queued. The exception was:\n{e}")

    async def start_consuming(self, pipeline_max_concurrency=16):
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(self._rabbitmq_queue, durable=True, arguments={'x-max-priority': 10})
        queue_iter = queue.iterator()
        async for message in queue_iter:
            try:
                await self._received_message(message,
                                             self._data_store_factory,
                                             self._metadata_store_factory,
                                             pipeline_max_concurrency,
                                             self._level)
            except aio_pika.exceptions.MessageProcessError:
                # Do not try to close() the queue iterator! If we get here, that means the RabbitMQ
                # connection has died, and attempting to close the queue will only raise another exception.
                raise RabbitMQLostConnectionError("Lost connection to RabbitMQ while processing a granule.")
            except Exception as e:
                await queue_iter.close()
                await channel.close()
                raise e
