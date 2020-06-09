import os
import time
import logging
import yaml
from typing import Callable


from config_operator.config_source.exceptions import UnreadableFileException

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

LISTEN_FOR_UPDATE_INTERVAL_SECONDS = 1


class LocalDirConfig:

    def __init__(self, local_dir: str,
                 update_every_seconds: int = LISTEN_FOR_UPDATE_INTERVAL_SECONDS):
        logger.info(f'create config on local dir {local_dir}')
        self._local_dir = local_dir
        self._latest_update = self._get_latest_update()
        self._update_every_seconds=update_every_seconds
        
    def get_files(self):
        files = []
        for f in os.listdir(self._local_dir):
            if os.path.isfile(os.path.join(self._local_dir, f)) \
                    and 'README' not in f \
                    and not f.startswith('.'):
                files.append(f)

        return files

    def _test_read_yaml(self, file_name: str):
        """ check yaml syntax raise yaml.parser.ParserError is it doesn't"""
        with open(os.path.join(self._local_dir, file_name), 'r') as f:
            docs = yaml.load_all(f, Loader=yaml.FullLoader)
            for doc in docs:
                pass

    def get_file_content(self, file_name: str):
        logger.info(f'read configuration file {file_name}')
        try:
            self._test_read_yaml(file_name)
            with open(os.path.join(self._local_dir, file_name), 'r') as f:
                return f.read()
        except UnicodeDecodeError as e:
            raise UnreadableFileException(e)
        except yaml.parser.ParserError as e:
            raise UnreadableFileException(e)


    def _get_latest_update(self):
        m_times = [os.path.getmtime(root) for root, _, _ in os.walk(self._local_dir)]
        if m_times:
            return time.ctime(max(m_times))
        else:
            return None

    def when_updated(self, callback: Callable[[], None]):
        """
          call function callback when the local directory is updated.
        """
        while True:
            time.sleep(self._update_every_seconds)
            latest_update = self._get_latest_update()
            if latest_update is None or (latest_update > self._latest_update):
                logger.info("local config dir has been updated")
                callback()
                self._latest_update = latest_update
            else:
                logger.debug("local config dir has not been updated")

        return None

