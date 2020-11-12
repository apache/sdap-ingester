from collection_manager.services import S3Observer
import unittest


class TestS3Observer(unittest.TestCase):

    def test_get_object_key(self):
        self.assertEqual('test_dir/object.nc', S3Observer._get_object_key('s3://test-bucket/test_dir/object.nc'))
