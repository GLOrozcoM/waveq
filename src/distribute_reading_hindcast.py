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


def get_s3_hindcast(csv_line):
    """ Acquire a single data set from an h5 file in an S3 bucket.

    :param csv_line: A single csv line to extract h5 data set from.
    :return: h5 data set transformed to list for rdd.
    """
    lines = csv_line.split(",")
    s3_endpoint = lines[0]
    # The AWS repository is public so AWS creds are not needed
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")

    # Get relevant info from h5 file
    metric = lines[1]
    slice_time = int(lines[2])
    slice_location = int(lines[3])
    data_set = h5_file[metric][0:slice_time, 0:slice_location]

    s3_file.close()
    h5_file.close()
    return list(data_set[()].tolist())


def read_s3_paths(file_path):
    """ Turn s3 file links found at file_path into a single rdd.

    :param file_path: File path from which to read h5 data sets from.
    :return: An rdd with a single row being a file link to an s3 file.
    """
    print("Reading in s3 file links at {}.".format(file_path))
    file_paths = sc.textFile(file_path)
    print("Finished reading file links at {}.".format(file_path))
    return file_paths


def single_metric_cycle(file_path):
    """ Take a single wave metric across four year chunks. Please see
    the s3_file_links directory for sample csv's.
    :return: A spark data frame containing the metric across all four years.
    """
    file_paths = read_s3_paths(file_path)

    print("Turning csv lines into rdd.")
    rdd = file_paths.flatMap(get_s3_hindcast)
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


    ## High level operation -> acquire data frames of variables
    # Could loop over different metrics and go for every four years.
    energy_path = "hindcast_links/energy_period_1979_to_1982.csv"
    energy_df = single_metric_cycle(energy_path)

    swh_path = "hindcast_links/significant_wave_height_1979_to_1982.csv"
    swh_df = single_metric_cycle(swh_path)

    ## High level operation -> perform joins

    # Perform an operation on the metric
    # Validate data
    print("The row count for this data frame is {}.".format(energy_df.count()))
    print("The row count for this data frame is {}.".format(swh_df.count()))
    print(energy_df.printSchema())
    print(swh_df.printSchema())
    sc.stop()

    end = time.time() - begin
    print("Entire process took {} seconds.".format(end))
