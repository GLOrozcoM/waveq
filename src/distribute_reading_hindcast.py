"""
Module to automate reading of h5 datasets into RDD format into Spark
-- ensure h5_names_paths_one.csv is on worker machines as well as the driver.
"""


import h5py
import s3fs
from ingest.ingest_s3 import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time


def get_s3_h5_files(csv_line):
    """

    :param csv_line:
    :param metric: The wave metric of interest (e.g. significant wave height, or energy period).
    :param slice_time: Upper bound on slice for times (max is 2918).
    :param slice_location: Upper bound on slice for locations (max is 699904 - not recommended).
    :return:
    """
    lines = csv_line.split(",")
    s3_endpoint = lines[0]
    # The AWS repository is public so AWS creds are not needed
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")
    # Take the metric of interest at specified time period over specified location.
    metric = lines[1]
    slice_time = int(lines[2])
    slice_location = int(lines[3])
    dataset = h5_file[metric][0:slice_time, 0:slice_location]
    return list(dataset[()].tolist())


def read_s3_paths(file_path):
    print("Reading in paths.")
    file_paths = sc.textFile(file_path)
    print("Finished reading in paths.")
    return file_paths


def single_metric_cycle(file_path):
    """

    :param metric:
    :param slice_time:
    :param slice_location:
    :return:
    """
    file_paths = read_s3_paths(file_path)
    print("Turning csv lines into rdd.")
    rdd = file_paths.flatMap(get_s3_h5_files)
    print("Successfully turned csv lines into rdd.")
    print("Going from RDD to data frame.")
    df = spark.createDataFrame(rdd)
    print("Finished going from RDD to data frame.")
    return df


if __name__ == "__main__":
    begin = time.time()

    # Spark setup
    sc = SparkContext(appName="Distribute reading of HDF5 files")
    spark = SparkSession(sc)

    # Could loop over different metrics and go for every four years.
    energy_path = "h5_names_paths_one.csv"
    energy_df = single_metric_cycle(energy_path)

    swh_path = "h5_names_paths_one.csv"
    swh_df = single_metric_cycle(swh_path)



    # Perform an operation on the metric
    # Validate data
    print("The row count for this data frame is {}.".format(energy_df.count()))
    print("The row count for this data frame is {}.".format(swh_df.count()))
    print(energy_df.printSchema())
    print(swh_df.printSchema())
    sc.stop()

    end = time.time() - begin
    print("Entire process took {} seconds.".format(end))