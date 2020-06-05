import unittest
import os

from config_operator.k8s import K8sConfigMap
from config_operator.config_source import RemoteGitConfig, LocalDirConfig


class ConfigMapTest(unittest.TestCase):
    def test_createconfigmapfromgit(self):

        remote_git_config = RemoteGitConfig("https://github.com/tloubrieu-jpl/sdap-ingester-config")
        
        config_map = K8sConfigMap('collection-ingester', 'sdap', remote_git_config)
        config_map.publish()

    def test_createconfigmapfromlocaldir(self):
        local_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '..',
                                 'resources')
        remote_git_config = LocalDirConfig(local_dir)

        config_map = K8sConfigMap('collection-ingester', 'sdap', remote_git_config)
        config_map.publish()


if __name__ == '__main__':
    unittest.main()
