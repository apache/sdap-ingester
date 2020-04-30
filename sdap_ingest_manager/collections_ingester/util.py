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

def read_local_configuration(config_path="/opt/sdap_ingester_config"):
    logger.info("====config====")
    config = configparser.ConfigParser()
    config.add_section("COLLECTIONS_YAML_CONFIG")
    config.set("COLLECTIONS_YAML_CONFIG", "config_path", config_path)
    candidates = [full_path('sdap_ingest_manager.ini.default'),
                  os.path.join(config_path, 'sdap_ingest_manager.ini')]
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