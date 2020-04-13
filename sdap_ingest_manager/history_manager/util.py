import logging
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BLOCK_SIZE = 65536


def md5sum_from_filepath(file_path):
    hasher = hashlib.md5()
    with open(file_path.strip(), 'rb') as afile:
        buf = afile.read(BLOCK_SIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCK_SIZE)
    return hasher.hexdigest()