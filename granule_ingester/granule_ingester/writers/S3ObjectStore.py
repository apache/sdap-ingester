import logging
from io import BytesIO
from uuid import UUID

import boto3
from granule_ingester.writers.DataStore import DataStore
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.DataTile_pb2 import TileData, NexusTile

logger = logging.getLogger(__name__)


class S3ObjectStore(DataStore):
    """
    Should be able to use for AWS-S3 and Ceph Object Store
    """

    def __init__(self, bucket, region, key=None, secret=None, session=None) -> None:
        super().__init__()
        self.__bucket = bucket
        self.__boto3_session = {
            'region_name': region,
        }
        if key is not None:
            self.__boto3_session['aws_access_key_id'] = key
        if secret is not None:
            self.__boto3_session['aws_secret_access_key'] = secret
        if session is not None:
            self.__boto3_session['aws_session_token'] = session
        self.__s3_client = boto3.Session(**self.__boto3_session).client('s3')

    async def health_check(self) -> bool:
        try:
            response = self.__s3_client.list_objects_v2(
                Bucket=self.__bucket,
                # Delimiter='string',
                # EncodingType='url',
                MaxKeys=10,
                Prefix='string',
                ContinuationToken='string',
                FetchOwner=False,
                # StartAfter='string',
                # RequestPayer='requester',
                # ExpectedBucketOwner='string'
            )
            # TODO inspect resopnse object
        except Exception as e:
            return False
        return True

    def save_data(self, nexus_tile: NexusTile) -> None:
        tile_id = str(UUID(str(nexus_tile.summary.tile_id)))
        logger.debug(f'saving data {tile_id}')
        serialized_tile_data = TileData.SerializeToString(nexus_tile.tile)
        logger.debug(f'uploading to object store')
        self.__s3_client.upload_fileobj(BytesIO(bytearray(serialized_tile_data)), self.__bucket, f'{tile_id}')
        return
