import logging
import os
import sys

from sdap_ingest_manager.history_manager import DatasetIngestionHistoryFile, DatasetIngestionHistorySolr

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


def create_history_manager(config, dataset_id):
    if config.has_section("HISTORY"):
        history_options_dict = get_conf_section_option_dict(config, "HISTORY")
        if 'history_path' in history_options_dict and 'solr_url' in history_options_dict:
            logger.error(
                "Either 'history_path' or 'solr_url' option should be supplied in HISTORY config, but not both.")
            return None
        if 'history_Path' in history_options_dict:
            return DatasetIngestionHistoryFile(full_path(history_options_dict["history_path"]), dataset_id)
        if 'solr_url' in history_options_dict:
            return DatasetIngestionHistorySolr(history_options_dict['solr_url'], dataset_id)
    else:
        return None
