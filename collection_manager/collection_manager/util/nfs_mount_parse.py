import os
import logging

logger = logging.getLogger(__name__)


def get_nfs_mount_points():
    """
    :return: a dictionary with local mount point as key (e.g /home/data)
    and targets nfs service as value (e.g. host2323:/export/data)
    """
    mount_points = {}

    logger.info("mounting point founds are:")
    for mount_str in os.popen('mount').read().split('\n'):
        mount_array = mount_str.split(' ')
        logger.info(mount_array)
        # depending on the system it is run on the mount command does not return the same format
        if len(mount_array) >= 4 and mount_array[4] == "nfs":
            mount_points[mount_array[2]] = mount_array[0]

    return mount_points


def replace_mount_point_with_service_path(file_path, mount_points):
    """
    :param file_path: is the file path as shown on the nfs client host
    :param mount_points: is the dictionary of locally mounted points as keys (e.g /home/data)
    and value the nfs service (e.g. host2323:/export/data)
    :return: the file path as seen on the nfs server.
    """
    for mount_point in mount_points:
        if file_path.startswith(mount_point):
            return file_path.replace(mount_point, mount_points[mount_point].split(":")[1], 1)
    return file_path  # return original value if it does not start with any of the mount point.


def replace_service_path_with_mount_point(file_path, mount_points):
    """
    :param file_path: is the file path as shown by the nfs server
    :param mount_points: is the dictionary of locally mounted points as keys (e.g /home/data)
    and value the nfs service (e.g. host2323:/export/data)
    :return: the file path as seen by the nfs client.
    """
    for mount_point, server_record in mount_points.items():
        service_path = server_record.split(":")[1]
        if file_path.startswith(service_path):
            return file_path.replace(service_path, mount_point, 1)
    return file_path  # return original value if it does not start with any of the mount point.
