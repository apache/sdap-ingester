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

import os
import logging
from kubernetes import client, config
from config_operator.config_source import LocalDirConfig, RemoteGitConfig
from kubernetes.client.rest import ApiException
from typing import Union
from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client import ApiClient
from config_operator.config_source.exceptions import UnreadableFileException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class K8sConfigMap:
    def __init__(self, configmap_name: str,
                 namespace: str,
                 external_config: Union[LocalDirConfig, RemoteGitConfig],
                 api_instance: ApiClient = None,
                 api_core_v1_instance: CoreV1Api = None):
        self._git_remote_config = external_config
        self._namespace = namespace
        self._configmap_name = configmap_name

        if api_core_v1_instance is None:
            # test is this runs inside kubernetes cluster
            if os.getenv('KUBERNETES_SERVICE_HOST'):
                config.load_incluster_config()
            else:
                config.load_kube_config()
            configuration = client.Configuration()
            api_instance = client.ApiClient(configuration)
            api_core_v1_instance = client.CoreV1Api(api_instance)
        self._api_instance = api_instance
        self._api_core_v1_instance = api_core_v1_instance
        self.publish()

    def __del__(self):
        self._api_instance.close()

    def _create_configmap_object(self):

        metadata = client.V1ObjectMeta(
            name=self._configmap_name ,
            namespace=self._namespace,
        )

        data = {}        
        for f in self._git_remote_config.get_files():
            try:
                data[f] = self._git_remote_config.get_file_content(f)
            except UnreadableFileException as e:
                logger.error(f'file {f} cannot be read, ignored', e)
 
        configmap = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            data=data,
            metadata=metadata
        )
        return configmap

    def _get_deployed_config(self):
        """
        This method does not work in my test, the list of config available is not up to date
        """
        try:
            api_list_response = self._api_core_v1_instance.list_namespaced_config_map(self._namespace)
            config_keys = set()
            for item in api_list_response.items:
                config_keys = config_keys.union(item.data.keys())
        except ApiException as e:
            logger.error("Exception when calling Kubernetes CoreV1Api %s\n" % e)
        finally:
            return config_keys

    def _patch(self):
        """ replaces files available in the config but does not delete
            what is not available (e.g. which has not been parsed)"""
        try:
            logger.info(f'replace configMap entry {self._configmap_name}')
            api_response = self._api_core_v1_instance.patch_namespaced_config_map(
                name=self._configmap_name,
                namespace=self._namespace,
                body=self._create_configmap_object()
            )
            logger.info(api_response)
        except ApiException as e:
            raise e

    def _create(self):
        try:
            logger.info(f'create configMap entry {self._configmap_name}')
            api_response = self._api_core_v1_instance.create_namespaced_config_map(
                namespace=self._namespace,
                body=self._create_configmap_object()
            )
            logger.info(api_response)

        except ApiException as e:
            raise e

    def publish(self):
        try:
            self._create()
        except ApiException as e:
            logger.error("Exception when calling Kubernetes CoreV1Api ,create failed, try to patch %s\n" % e)
            self._patch()

        return None

