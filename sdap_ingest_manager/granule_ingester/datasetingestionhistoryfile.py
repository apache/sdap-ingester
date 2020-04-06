import os
from pathlib import Path
import logging


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DatasetIngestionHistoryFile:
    _dataset_id = None
    _history_file_path = None
    _history_file = None
    _history_dict = {}

    def __init__(self, history_path, dataset_id):
        self._dataset_id = dataset_id
        self._history_file_path = os.path.join(history_path, f'{dataset_id}.csv')
        self._load_history_dict()
        Path(history_path).mkdir(parents=True, exist_ok=True)
        self._history_file = open(self._history_file_path, 'a')

    def _load_history_dict(self):
        logger.info(f"loading history file {self._history_file_path}")
        try:
            with open(self._history_file_path, 'r') as f_history:
                for line in f_history:
                    filename, md5sum = line.strip().split(',')
                    logger.info(f"add to history file {filename} with md5sum {md5sum}")
                    self._history_dict[filename] = md5sum
        except FileNotFoundError:
            logger.info("no history file created yet")

    def __del__(self):
        self._history_file.close()
        del self._history_dict

    def push(self, file_name, md5sum):
        # need to find a way to delete previous occurence of the file if it has been loaded before
        self._history_file.write(f'{file_name},{md5sum}\n')
        return None

    def get_md5sum(self, file_name):
        if file_name in self._history_dict.keys():
            return self._history_dict[file_name]
        else:
            return None
