import configparser
import logging
import os

from sdap_ingest_manager.util.util import full_path

logger = logging.getLogger(__name__)


class LocalConfiguration:
    def __init__(self, config_path="/opt/sdap_ingester_config"):
        self._config_path = config_path
        self._config = self._read_local_configuration()

    def get(self):
        return self._config

    def save(self):
        logger.info("save configuration")
        local_config_file_path = os.path.join(self._config_path, 'sdap_ingest_manager.ini')
        with open(local_config_file_path, 'w') as f:
            self._config.write(f)
        logger.info(f"successfully saved configuration to {self._config_path}")

    def _read_local_configuration(self):
        logger.info("====config====")
        config = configparser.ConfigParser()
        config.add_section("COLLECTIONS_YAML_CONFIG")
        config.set("COLLECTIONS_YAML_CONFIG", "config_path", self._config_path)
        candidates = [full_path('sdap_ingest_manager.ini.default'),
                      os.path.join(self._config_path, 'sdap_ingest_manager.ini')]
        logger.info(f"get configuration from files {candidates}")
        found_files = config.read(candidates)
        logger.info(f"successfully read configuration from {found_files}")
        return config
