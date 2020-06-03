import unittest
import os

from flask import Flask
from flask_restplus import Api

from sdap_ingest_manager.config.ConfigMap import ConfigMap
from sdap_ingest_manager.config.RemoteGitConfig import RemoteGitConfig


class ConfigMapTest(unittest.TestCase):
    def test_createconfigmap(self):

        remote_git_config = RemoteGitConfig("https://github.com/tloubrieu-jpl/sdap-ingester-config")
        
        config_map = ConfigMap('collection-ingester', 'sdap', remote_git_config)
        config_map.publish()



if __name__ == '__main__':
    unittest.main()
