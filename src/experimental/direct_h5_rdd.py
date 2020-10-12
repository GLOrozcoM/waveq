"""
Module to automate reading of h5 datasets into RDD format into Spark
-- ensure h5_names_paths_one.csv is on worker machines as well as the driver.
"""

# Code modifed from https://www.hdfgroup.org/2015/03/from-hdf5-datasets-to-apache-spark-rdds/

import h5py
import s3fs
from ingest.ingest_s3 import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time

sc = SparkContext(appName="SparkHDF5")
spark = SparkSession(sc)
partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
begin = time.time()

def get_s3_h5_files(csv_line):
    """ Use as a flat map function to distribute reading of a file.

    :param csv_line: Encapsulates an s3 file path to an .h5 ocean wave file and internal data set path.
    :return:
    """
    lines = csv_line.split(",")
    s3_endpoint = lines[0]
    # -- check if not having three separate lines for a separate machine
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")

    # Each h5_file has multiple data sets
    # -- know for a fact 'coordinates' exists in file
    dataset = h5_file['energy_period'][0:2918, 0:1000]
    return list(dataset[()].tolist())

print("Reading in to text file")
file_paths = sc.textFile("h5_names_paths_one.csv")
print("Finished reading in to text file")


print("Turning csv lines into rdd")
rdd = file_paths.flatMap(get_s3_h5_files)
print("Successfully turned csv lines into rdd")

df = spark.createDataFrame(rdd)
print("See the count of these data frames")
print(df.count())

sc.stop()

end = time.time() - begin
print("Entire process took {} seconds.".format(end))