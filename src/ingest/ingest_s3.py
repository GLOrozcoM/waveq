"""
Module to encapsulate getting data from s3.
"""
import s3fs
import h5py


def call_s3_to_h5(s3_endpoint):
    """ Get h5 file from s3 with an endpoint.

    :param s3_endpoint:
    :return:
    """
    s3 = s3fs.S3FileSystem()
    h5_file = h5py.File(s3.open(s3_endpoint,"rb"), "r")
    return h5_file