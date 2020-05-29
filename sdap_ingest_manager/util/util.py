import logging
import os
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def full_path(relative_path):
    sdap_ingest_manager_home = os.path.join(sys.prefix, '.sdap_ingest_manager')
    return os.path.join(sdap_ingest_manager_home,
                        relative_path)
