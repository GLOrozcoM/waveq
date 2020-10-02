"""
Module to automate reading of h5 datasets into RDD format into Spark
-- ensure h5_names_paths.csv is on worker machines as well as the driver.
"""

# Code modifed from https://www.hdfgroup.org/2015/03/from-hdf5-datasets-to-apache-spark-rdds/

import h5py
import sys
import boto3
from fs import open_fs
import s3fs
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext(appName="SparkHDF5")
spark = SparkSession(sc)
partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

def get_s3_h5_files(csv_line):
    """ Use as a flat map function to distribute reading of a file.

    :param csv_line: Encapsulates an s3 file path to an .h5 ocean wave file and internal dataset path.
    :return:
    """
    lines = csv_line.split(",")

    # BOTO3 attempts
    # -- placeholder code, not expecting to run
    s3_resource = 'settings'

    bucket = s3_resource.Bucket("wpto-pds-us-wave")
    obj = bucket.Object(key='v1.0.0/virtual_buoy/US_virtual_buoy_2010.h5')

    # Time out occurring here
    # -- access byte stream for reads
    s3_file = obj.get()['Body'].read()

    h5_file = h5py.File(s3_file, "r")
    result = h5_file[lines[1]]
    return list(result[:])

print("Reading in to text file")
file_paths = sc.textFile("h5_names_paths.csv", minPartitions=partitions)
print("Finished reading in to text file")

print("Turning csv lines into rdd")
rdd = file_paths.flatMap(get_s3_h5_files)
print("Successfully turned csv lines into rdd")

df = spark.createDataFrame(rdd)
print(df)

sc.stop()