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
import os
import tempfile
from urllib import parse

import aioboto3
import xarray as xr

from granule_ingester.exceptions import GranuleLoadingError

logger = logging.getLogger(__name__)


class GranuleLoader:

    def __init__(self, resource: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._granule_temp_file = None
        self._resource = resource

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, type, value, traceback):
        if self._granule_temp_file:
            self._granule_temp_file.close()

    async def open(self) -> (xr.Dataset, str):
        resource_url = parse.urlparse(self._resource)
        if resource_url.scheme == 's3':
            # We need to save a reference to the temporary granule file so we can delete it when the context manager
            # closes. The file needs to be kept around until nothing is reading the dataset anymore.
            self._granule_temp_file = await self._download_s3_file(self._resource)
            file_path = self._granule_temp_file.name
        elif resource_url.scheme == '':
            file_path = self._resource
        else:
            raise RuntimeError("Granule path scheme '{}' is not supported.".format(resource_url.scheme))

        granule_name = os.path.basename(self._resource)
        try:
            return xr.open_dataset(file_path, lock=False), granule_name
        except FileNotFoundError:
            raise GranuleLoadingError(f"The granule file {self._resource} does not exist.")
        except Exception:
            raise GranuleLoadingError(f"The granule {self._resource} is not a valid NetCDF file.")

    @staticmethod
    async def _download_s3_file(url: str):
        parsed_url = parse.urlparse(url)
        logger.info(
            "Downloading S3 file from bucket '{}' with key '{}'".format(parsed_url.hostname, parsed_url.path[1:]))
        async with aioboto3.resource("s3") as s3:
            obj = await s3.Object(bucket_name=parsed_url.hostname, key=parsed_url.path[1:])
            response = await obj.get()
            data = await response['Body'].read()
            logger.info("Finished downloading S3 file.")

        fp = tempfile.NamedTemporaryFile()
        fp.write(data)
        logger.info("Saved downloaded file to {}.".format(fp.name))
        return fp
