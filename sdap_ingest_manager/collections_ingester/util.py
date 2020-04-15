import configparser
import os
import sys
import logging
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def full_path(relative_path):
    sdap_ingest_manager_home = os.path.join(sys.prefix, '.sdap_ingest_manager')
    return os.path.join(sdap_ingest_manager_home,
                        relative_path)


def read_local_configuration():
    logger.info("====config====")
    config = configparser.ConfigParser()
    candidates = [full_path('sdap_ingest_manager.ini.default'),
                  full_path('sdap_ingest_manager.ini')]
    logger.info(f"get configuration from files {candidates}")
    found_files = config.read(candidates)
    logger.info(f"successfully read configuration from {found_files}")
    return config