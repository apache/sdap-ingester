import unittest
from sdap_ingest_manager.collections_ingester import nfs_mount_parse


class TestUnitMgr(unittest.TestCase):
    mount_points = {"/home/data": "srv:/export/data",
                    "/home/metadata": "srv2:/export/metadata"}

    def test_get_nfs_mount_points(self):
        mount = nfs_mount_parse.get_nfs_mount_points()
        self.assertGreaterEqual(len(mount), 0)  # not much to verify as it depends on the host

    def test_replace_mount_point_with_service_path(self):

        result = nfs_mount_parse.replace_mount_point_with_service_path("/home/data/file1", self.mount_points)
        self.assertEqual(result, "/export/data/file1")

    def test_dont_replace_mount_point_with_service_path(self):

        result = nfs_mount_parse.replace_mount_point_with_service_path("/home1/data/file1", self.mount_points)
        self.assertEqual(result, "/home1/data/file1")


if __name__ == '__main__':
    unittest.main()
