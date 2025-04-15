import logging
import os
import tempfile
from urllib import parse

import aioboto3
import xarray as xr
from granule_ingester.exceptions import GranuleLoadingError, PipelineBuildingError
from granule_ingester.granule_loaders.Preprocessors import modules as module_mappings
from granule_ingester.preprocessors import GranulePreprocessor

logger = logging.getLogger(__name__)


class GranuleLoader:

    def __init__(self, resource: str, *args, **kwargs):
        self._granule_temp_file = None
        self._resource = resource
        self._preprocess = None

        self._group = kwargs.get('group', None)
        self._group_vars = kwargs.get('grouped_vars', [])

        if 'preprocess' in kwargs:
            self._preprocess = [GranuleLoader._parse_module(module) for module in kwargs['preprocess']]

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, type, value, traceback):
        if self._granule_temp_file:
            self._granule_temp_file.close()

    async def open(self) -> (xr.Dataset, str):
        resource_url = parse.urlparse(self._resource)

        if resource_url.scheme == 's3':
            self._granule_temp_file = await self._download_s3_file(self._resource)
            file_path = self._granule_temp_file.name
        else:
            # Local file → Use raw path directly
            file_path = self._resource

        granule_name = os.path.basename(self._resource)

        try:
            additional_params = {}
            if self._group is not None:
                additional_params['group'] = self._group

            ds = xr.open_dataset(file_path, lock=False, engine="netcdf4", **additional_params)

            for group_var in self._group_vars:
                parts = group_var.split('/')
                group = '/'.join(parts[:-1])
                var_name = parts[-1]
                ds_grp = xr.open_dataset(file_path, lock=False, group=group, engine="netcdf4")
                ds[group_var] = ds_grp[var_name]

            if self._preprocess:
                logger.info(f'Applying {len(self._preprocess)} preprocessors to granule {self._resource}')
                while self._preprocess:
                    preprocessor: GranulePreprocessor = self._preprocess.pop(0)
                    ds = preprocessor.process(ds)

            return ds, granule_name

        except FileNotFoundError:
            raise GranuleLoadingError(f"The granule file {self._resource} does not exist.")
        except Exception as e:
            logger.exception("Failed to open NetCDF file")
            raise GranuleLoadingError(f"The granule {self._resource} is not a valid NetCDF file.")

    @staticmethod
    async def _download_s3_file(url: str):
        parsed_url = parse.urlparse(url)
        logger.info(f"Downloading S3 file from bucket '{parsed_url.hostname}' with key '{parsed_url.path[1:]}'")
        async with aioboto3.resource("s3") as s3:
            obj = await s3.Object(bucket_name=parsed_url.hostname, key=parsed_url.path[1:])
            response = await obj.get()
            data = await response['Body'].read()

        fp = tempfile.NamedTemporaryFile()
        fp.write(data)
        logger.info(f"Saved downloaded file to {fp.name}.")
        return fp

    @staticmethod
    def _parse_module(module_config: dict):
        module_name = module_config.pop('name')
        try:
            module_class = module_mappings[module_name]
            logger.debug(f"Loaded preprocessor {module_class}.")
            return module_class(**module_config)
        except KeyError:
            raise PipelineBuildingError(f"'{module_name}' is not a valid preprocessor.")
        except Exception as e:
            raise PipelineBuildingError(f"Parsing module '{module_name}' failed: {e}")
