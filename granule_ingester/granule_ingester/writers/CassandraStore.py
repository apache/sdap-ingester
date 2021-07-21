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


import asyncio
import logging
import uuid
import time

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session, NoHostAvailable
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.policies import RetryPolicy, ConstantReconnectionPolicy
from nexusproto.DataTile_pb2 import NexusTile, TileData

from granule_ingester.exceptions import CassandraFailedHealthCheckError, CassandraLostConnectionError
from granule_ingester.writers.DataStore import DataStore
from granule_ingester.writers.CassandraStoreConnectionRetryPolicy import CassandraStoreConnectionRetryPolicy

logging.getLogger('cassandra').setLevel(logging.INFO)
logger = logging.getLogger(__name__)


class TileModel(Model):
    __keyspace__ = "nexustiles"
    __table_name__ = "sea_surface_temp"
    tile_id = columns.UUID(primary_key=True)
    tile_blob = columns.Bytes(index=True)


class CassandraStore(DataStore):
    def __init__(self, contact_points=None, port=9042, username=None, password=None):
        self._contact_points = contact_points
        self._username = username
        self._password = password
        self._port = port
        self._session = None

    async def health_check(self) -> bool:
        try:
            session = self._get_session()
            session.shutdown()
            return True
        except Exception:
            raise CassandraFailedHealthCheckError("Cannot connect to Cassandra!")

    def _get_session(self) -> Session:

        if self._username and self._password:
            auth_provider = PlainTextAuthProvider(username=self._username, password=self._password)
        else:
            auth_provider = None

        cluster = Cluster(contact_points=self._contact_points,
                          port=self._port,
                          #load_balancing_policy=DCAwareRoundRobinPolicy("dc1"),
                          protocol_version=4,
                          reconnection_policy=ConstantReconnectionPolicy(delay=5.0),
                          default_retry_policy=CassandraStoreConnectionRetryPolicy(),
                          auth_provider=auth_provider)

        session = cluster.connect()
        session.set_keyspace('nexustiles')
        session.default_timeout = 60

        return session

    def connect(self):
        self._session = self._get_session()

    def __del__(self):
        if self._session:
            self._session.shutdown()

    async def save_data(self, tile: NexusTile, max_num_try=6, num_try=0) -> None:
        try:
            tile_id = uuid.UUID(tile.summary.tile_id)
            serialized_tile_data = TileData.SerializeToString(tile.tile)
            prepared_query = self._session.prepare("INSERT INTO sea_surface_temp (tile_id, tile_blob) VALUES (?, ?)")
            logger.debug("starting to updload tile %s data on cassandra", tile_id)
            await self._execute_query_async(self._session, prepared_query,
                                            [tile_id, bytearray(serialized_tile_data)])
        except Exception as e:
            if max_num_try >= num_try:
                time.sleep(2**num_try)
                logger.warning("exception while uploading tile data on cassandra %s, retry once more", e)
                await self.save_data(tile, max_num_try=max_num_try, num_try=num_try+1)
            else:
                logger.error("exception while uploading tile data on cassandra %s, second attempt", e)
                raise CassandraLostConnectionError(f"Lost connection to Cassandra, and cannot save tiles.")


    @staticmethod
    async def _execute_query_async(session: Session, query, parameters=None):
        cassandra_future = session.execute_async(query, parameters, timeout=6000)
        asyncio_future = asyncio.Future()
        cassandra_future.add_callbacks(asyncio_future.set_result, asyncio_future.set_exception)
        return await asyncio_future
