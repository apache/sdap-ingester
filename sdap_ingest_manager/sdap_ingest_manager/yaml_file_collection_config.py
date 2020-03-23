import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_yaml_collection_config(file, collection_callback):
    """
    Read the collection configuration in a yaml file and apply row_callback on each collection
    :param file: yaml file containing the configurations of the collections
    :param collection_callback: function which is applied to each collection, take as parameter a tuple [collection_id, netcdf variable, netcdf file path pattern]
    :return:
    """
    with open(file, 'r') as f:
        collections = yaml.load(f, Loader=yaml.FullLoader)

    for collection in collections.keys():
        path = collections[collection]['path']
        variable = collections[collection]['variable']
        collection_callback([collection, variable, path])
