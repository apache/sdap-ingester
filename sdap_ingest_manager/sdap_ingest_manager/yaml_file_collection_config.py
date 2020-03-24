import logging
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_yaml_collection_config(file, collection_callback):
    """
    Read the collection configuration in a yaml file, sort them by priority order
    and apply row_callback on each collection
    :param file: yaml file containing the configurations of the collections
    :param collection_callback: function which is applied to each collection, take as parameter a dictionary
    with keys [id, variable, path]
    :return:
    """
    with open(file, 'r') as f:
        collections = yaml.load(f, Loader=yaml.FullLoader)

    collections_array = []
    for (c_id, collection) in collections.items():
        collection['id'] = c_id
        for k, v in collection.items():
            if type(v) == str:
                collection[k] = v.strip()
        collections_array.append(collection)

    logger.info(f"collections are {collections_array}")
    sorted_collections = sorted(collections_array, key=lambda c: c['priority'])

    for collection in sorted_collections:
        collection_callback(collection)
