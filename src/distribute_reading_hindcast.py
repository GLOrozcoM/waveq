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
import numpy as np
import pandas as pd


def h5_time_to_pd_to_spark(data_set):
    """ Take in an h5 time stamped dataset. Output a spark df.

    :param data_set:
    :return:
    """
    converted_time_np = [np.datetime64(entry) for entry in data_set]
    time_pd = pd.DataFrame(converted_time_np)
    sp_time = spark.createDataFrame(time_pd)
    return sp_time


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


def create_metric_df(file_path):
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


def h5_to_spark(data_set):
    """ Convert an h5 data set into a spark df.

    :param data_set: An h5 data set.
    :return: A spark df.
    """
    list_dataset = data_set.tolist()
    rdd_dataset = sc.parallelize(list_dataset)
    sp_dataset = rdd_dataset.toDF()
    return sp_dataset


def create_df_coords(s3_endpoint):
    # The AWS repository is public so AWS creds are not needed
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")
    coord_h5 = h5_file['coordinates']
    coord_df = h5_to_spark(coord_h5[:])

    s3_file.close()
    h5_file.close()
    return coord_df


def turn_metrics_into_df(base_file_path):
    """ Turn selected wave metrics into spark df.

    NOTE time and coordinates are dealt with separately since their readings do not change
    over the years.

    :return: A dictionary of spark df's.
    """
    data_set_list = data_sets = ['directionality_coefficient', 'energy_period', 'maximum_energy_direction',
                                 'omni-directional_wave_power', 'significant_wave_height', 'spectral_width']
    metric_df_dict = {}
    all_metrics_begin = time.time()
    print("Starting to get all metrics")
    for data_set in data_set_list:
        file_path = base_file_path + data_set + "_1979_to_1982.csv"
        print("Getting data set {} at file path {}.".format(data_set, file_path))
        metric_df = create_metric_df(file_path)
        metric_df_dict[data_set] = metric_df
    all_metrics_end = time.time() - all_metrics_begin
    print("Ended getting all metrics. Process took {} seconds".format(all_metrics_end))
    return metric_df_dict


if __name__ == "__main__":
    begin = time.time()

    # Spark setup
    sc = SparkContext(appName="Distribute reading of HDF5 files")
    spark = SparkSession(sc)


    ## High level operation -> acquire data frames of variables

    # Manually generate time points here
    print("Creating date times data frame.")
    np_time = np.arange(np.datetime64('1979-01-01'), np.datetime64('1982-12-31'), np.timedelta64(3, 'h'))
    pd_time = pd.DataFrame(np_time)
    sp_time = spark.createDataFrame(pd_time)
    print("Finished creating date times data frame.")

    sp_time.show(10)

    base_file_path = "hindcast_links/"
    metrics = turn_metrics_into_df(base_file_path)

    # Coordinates don't change across years
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_1979.h5"
    print("Getting coordinates")
    coord_df = create_df_coords(s3_endpoint)
    print("Completed getting coordinates")


    ## High level operation -> perform joins
    # -- join each metric with time

    sc.stop()

    end = time.time() - begin
    print("Entire process took {} seconds.".format(end))
