import configparser
import os
import sys
import logging
from sdap_ingest_manager.history_manager import DatasetIngestionHistoryBuilder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def full_path(relative_path):
    sdap_ingest_manager_home = os.path.join(sys.prefix, '.sdap_ingest_manager')
    return os.path.join(sdap_ingest_manager_home,
                        relative_path)


def get_conf_section_option_dict(config, section):
    """
    :param config: config object from configParser
    :param section: section of the config file
    :return: a dictionnary of the options and their values
    """
    return {k: config.get(section, k) for k in config.options(section)}


class ConfigWithPath(configparser.ConfigParser):

    PATH_OPTIONS_SUFFIX = {'_dir', '_file'}

    def read(self, filenames):
        """ replaces every configuration option ending with '_dir' or '_file' with the aboslute path based on the first item path in filenames argument
            this makes every option absolute paths while we suppose that they might be relative path based on path of the current config file
        """
        super().read(filenames)
        absolute_dir = os.path.join(os.getcwd(),
                                     os.path.dirname(filenames[0]))
        for section in self.sections():
            for option in self.options(section):
                if option.endswith("_file") or option.endswith("_dir"):
                    new_option_value =  os.path.join(absolute_dir, self.get(section, option).strip())
                    self.set(section, option, new_option_value)


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

def create_history_manager_builder(_config):
    if _config.has_section("HISTORY"):
        history_options_dict = get_conf_section_option_dict(_config, "HISTORY")
        if "history_path" in history_options_dict.keys():
            history_options_dict["history_path"] = full_path(history_options_dict["history_path"])
        return DatasetIngestionHistoryBuilder(**history_options_dict)
    else:
        return None