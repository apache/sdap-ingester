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

import unittest
from unittest.mock import Mock
import os
from kubernetes.client.rest import ApiException

from config_operator.k8s import K8sConfigMap
from config_operator.config_source import RemoteGitConfig, LocalDirConfig

if 'GIT_USERNAME' in os.environ:
    GIT_USERNAME = os.environ['GIT_USERNAME']
if 'GIT_TOKEN' in os.environ:
    GIT_TOKEN = os.environ['GIT_TOKEN']


class K8sConfigMapTest(unittest.TestCase):
    @unittest.skip('requires remote git')
    def test_createconfigmapfromgit(self):

        remote_git_config = RemoteGitConfig("https://github.com/tloubrieu-jpl/sdap-ingester-config")
        
        config_map = K8sConfigMap('collection-ingester', 'sdap', remote_git_config)
        config_map.publish()

    @unittest.skip('requires remote git')
    def test_createconfigmapfromgit_with_token(self):
        remote_git_config = RemoteGitConfig("https://podaac-git.jpl.nasa.gov:8443/podaac-sdap/deployment-configs.git",
                                            git_username=GIT_USERNAME,
                                            git_token=GIT_TOKEN)

        config_map = K8sConfigMap('collection-ingester', 'sdap', remote_git_config)
        config_map.publish()

    @unittest.skip('requires kubernetes')
    def test_createconfigmapfromlocaldir_with_k8s(self):
        local_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '..',
                                 'resources',
                                 'localDirTest')
        local_config = LocalDirConfig(local_dir)

        config_map = K8sConfigMap('collection-ingester', 'sdap', local_config)
        config_map.publish()

    def test_createconfigmapfromlocaldir(self):
        local_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '..',
                                 'resources',
                                 'localDirTest')
        local_config = LocalDirConfig(local_dir)

        api_instance = Mock()
        api_instance.close = Mock()

        api_core_v1_mock = Mock()
        api_core_v1_mock.create_namespaced_config_map = Mock(return_value={
            'api_version': 'v1',
            'binary_data': None,
            'data': {}
        })
        api_core_v1_mock.patch_namespaced_config_map = Mock(return_value={
            'api_version': 'v1',
            'binary_data': None,
            'data': {}
        })
        api_core_v1_mock.create_namespaced_config_map.side_effect = Mock(side_effect=ApiException('409'))

        config_map = K8sConfigMap('collection-ingester', 'sdap', local_config,
                                  api_instance = api_instance,
                                  api_core_v1_instance=api_core_v1_mock)
        config_map.publish()


if __name__ == '__main__':
    unittest.main()
