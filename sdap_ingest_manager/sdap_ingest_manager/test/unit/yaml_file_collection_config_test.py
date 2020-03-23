import unittest
import pathlib
import os
from sdap_ingest_manager.sdap_ingest_manager import read_yaml_collection_config


def collection_callback_counter(row, n_collection):
    print(row)


class MyTestCase(unittest.TestCase):
    n_collection = 0

    def test_read_yaml_config(self):
        yaml_config_file = os.path.join(pathlib.Path(__file__).parent.absolute(),
                                        '..',
                                        'data',
                                        'collections.yml')

        def collection_callback(row):
            collection_callback_counter(row, self.n_collection)
            self.n_collection += 1

        read_yaml_collection_config(yaml_config_file, collection_callback)
        self.assertEqual(self.n_collection, 2)


if __name__ == '__main__':
    unittest.main()
