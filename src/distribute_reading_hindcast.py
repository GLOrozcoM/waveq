"""
Module to automate reading of h5 datasets into RDD format into Spark
-- ensure h5_names_paths_one.csv is on worker machines as well as the driver.
"""

import h5py
import s3fs
from ingest.ingest_s3 import *
from database.rdd_spark_to_db import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
import numpy as np
import pandas as pd
from pyspark.sql.functions import monotonically_increasing_id


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
    begin = time.time()
    file_paths = sc.textFile(file_path)
    end = time.time() - begin
    print("Finished reading file links at {} in {} seconds.".format(file_path, end))
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
    begin = time.time()
    df = spark.createDataFrame(rdd)
    end = time.time() - begin
    print("Finished going from RDD to data frame in {} seconds.".format(end))
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
    """

    :param s3_endpoint:
    :return:
    """
    # The AWS repository is public so AWS creds are not needed
    print("Getting coordinates")
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")
    coord_h5 = h5_file['coordinates']
    coord_df = h5_to_spark(coord_h5[:])
    print("Completed getting coordinates")
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


def generate_time_index(start_year, end_year):
    """ Create a time index at a three hour resolution for given years.

    :return: A spark df with time indices.
    """
    print("Creating date times data frame.")
    np_time = np.arange(np.datetime64(str(start_year) + '-01-01'), np.datetime64(str(end_year) + '-12-31'), np.timedelta64(3, 'h'))
    pd_time = pd.DataFrame(np_time)
    sp_time = spark.createDataFrame(pd_time)
    print("Finished creating date times data frame.")
    return sp_time


def give_id(metric_df):
    """ Assign an id column to a selected power spark df.

    :param metric_df: Spark df with power metric.
    :return: A power spark df with an id.
    """
    metric_with_id = metric_df.withColumn("id_key", monotonically_increasing_id())
    return metric_with_id


def give_all_metrics_id(metrics):
    """ Give all metrics an id to join with time.

    :param metrics:
    :return:
    """
    metrics_with_id = {}
    for key in metrics:
        metrics_with_id[key] = give_id(metrics[key])
    return metrics_with_id


def join_time_to_metrics(metrics_with_id):
    """ Give each wave metric a time index to query on.

    :param metrics_with_id:
    :return: A dictionary containing time indexed metrics.
    """
    print("Starting to join all metrics with a time index.")
    join_begin = time.time()
    time_index_metrics = {}
    for key in metrics_with_id:
        time_index_metrics[key] = metrics_with_id[key].join(time_df, on="id_key")
    join_end = time.time() - join_begin
    print("Completed joining all metrics with a time index. Took {} seconds to complete.".format(join_end))
    return time_index_metrics


def write_to_db(db_name, metrics_with_time_index, coordinates):
    print("Writing coordinates to db.")
    write_to_postgres(db_name, coordinates, "coordinates")
    print("Writing coordinates ended.")
    for key in metrics_with_time_index:
        table_name = key
        data_frame = metrics_with_time_index[key]
        print("Writing {} to postgres.".format(table_name))
        begin_write = time.time()
        write_to_postgres(db_name, data_frame, table_name)
        end_write = time.time() - begin_write
        print("Writing {} to postgres ended in {} seconds.".format(table_name, end_write))


if __name__ == "__main__":
    begin = time.time()

    # Spark setup
    sc = SparkContext(appName="Distribute reading of HDF5 files")
    spark = SparkSession(sc)

    # Reduce information printed on spark terminal
    sc.setLogLevel("ERROR")

    ## High level operation -> acquire data frames of variables
    # Manually generate time points here
    start_year = 1979
    end_year = 1982
    time_df = generate_time_index(start_year, end_year)
    time_df = give_id(time_df)

    # Take coordinates from 1979 since coordinates won't change across years
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_1979.h5"
    coord_df = create_df_coords(s3_endpoint)

    # Get relevant wave metrics
    base_file_path = "hindcast_links/"
    metrics = turn_metrics_into_df(base_file_path)
    metrics_with_id = give_all_metrics_id(metrics)

    ## High level operation -> perform joins
    # -- join each metric with time
    metrics_with_time_index = join_time_to_metrics(metrics_with_id)

    # Write to postgresql
    db_name = "hindcast_test"
    write_to_db(db_name, metrics_with_time_index, coord_df)

    sc.stop()

    end = time.time() - begin
    print("Entire process took {} seconds.".format(end))
