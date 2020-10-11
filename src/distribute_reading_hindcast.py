"""
Module to automate reading of h5 datasets into RDD format into Spark
-- ensure h5_names_paths_one.csv is on worker machines as well as the driver.

Entire process for 4 years takes 496 seconds.
For a single data set write to db it is 70 seconds (max bound).
Turning a single rdd to a df can take up to 20 seconds.
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


def h5_time_to_pd_to_spark(data_set):
    """ Take in an h5 time stamped data set. Output a spark df.

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
    lines_begin = time.time()
    rdd = file_paths.flatMap(get_s3_hindcast)
    end_lines = time.time() - lines_begin
    print("Successfully turned csv lines into rdd in {} seconds.".format(end_lines))


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
    list_data_set = data_set.tolist()
    rdd_data_set = sc.parallelize(list_data_set)
    sp_data_set = rdd_data_set.toDF()
    return sp_data_set


def create_df_coords(s3_endpoint):
    """

    :param s3_endpoint:
    :return:
    """
    print("Getting coordinates")
    begin = time.time()
    # The AWS repository is public so AWS creds are not needed
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")
    coord_h5 = h5_file['coordinates']
    coord_df = h5_to_spark(coord_h5[:])
    end = time.time() - begin
    print("Completed getting coordinates in {} seconds".format(end))
    s3_file.close()
    h5_file.close()
    return coord_df


def turn_metrics_into_df(base_file_path, start_year, end_year):
    """ Turn selected wave metrics into spark df.

    NOTE time and coordinates are dealt with separately since their readings do not change
    over the years.

    :return: A dictionary of spark df's.
    """
    # Notice 'omni-directional_wave_power' must have '-' otherwise won't be read in.
    data_set_list = ['directionality_coefficient', 'energy_period', 'maximum_energy_direction',
                      'omni-directional_wave_power', 'significant_wave_height', 'spectral_width']
    metric_df_dict = {}
    all_metrics_begin = time.time()
    print("Starting to get all metrics")
    for data_set in data_set_list:
        file_path = base_file_path + data_set + "_" + str(start_year) + "_to_" + str(end_year) + ".csv"
        print("Getting data set {} at file path {}.".format(data_set, file_path))
        metric_df = create_metric_df(file_path)
        metric_df_dict[data_set] = metric_df
    all_metrics_end = time.time() - all_metrics_begin
    print("Ended getting all metrics. Process took {} seconds".format(all_metrics_end))
    return metric_df_dict


def get_time_index_hindcast(start_year):
    """ Access time indices for data sets.
    :param start_year: Year to start getting data for (min 1979, max 2006).
    :return: A spark df with time indices.
    """
    print("Accessing time data frames from s3.")
    begin = time.time()

    # Single starting year
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_" + str(start_year) + ".h5"
    s3 = s3fs.S3FileSystem(anon=True)
    s3_file = s3.open(s3_endpoint, "rb")
    h5_file = h5py.File(s3_file, "r")
    time_h5 = h5_file['time_index'][0:2918]
    time_df = h5_time_to_pd_to_spark(time_h5)
    s3_file.close()
    h5_file.close()

    # Combine years
    for offset in range(1, 4):
        s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_" + str(start_year + offset) + ".h5"
        s3 = s3fs.S3FileSystem(anon=True)
        s3_file = s3.open(s3_endpoint, "rb")
        h5_file = h5py.File(s3_file, "r")
        time_h5 = h5_file['time_index'][0:2918]
        new_time_df = h5_time_to_pd_to_spark(time_h5)
        time_df = time_df.union(new_time_df)
        s3_file.close()
        h5_file.close()
    end = time.time() - begin
    print("Completed getting data frame from s3 in {} seconds.".format(end))
    return time_df


def give_id(metric_df):
    """ Assign an id column to a spark df.

    :param metric_df: Spark df with power metric.
    :return: A power spark df with an id.
    """
    print("Starting to zip indices.")
    begin_zip = time.time()
    rdd_df = metric_df.rdd.zipWithIndex()
    end_zip = time.time() - begin_zip
    print("Completed zipping indices in {} seconds.".format(end_zip))

    print("Going from zipped rdd to df.")
    begin_df = time.time()
    metric_df = rdd_df.toDF()
    end_df = time.time() - begin_df
    print("Completed going from zipped rdd to df in {} seconds.".format(end_df))

    # Rename index
    metric_df = metric_df.withColumnRenamed('_2', "id_key")

    print("Starting to unpack columns from zipped format.")
    unpack_begin = time.time()
    # 1000 locations in original slice
    for i in range(1, 1001):
        # Note that '_1' doesn't change when accessing the columns
        metric_df = metric_df.withColumn('loc_' + str(i), metric_df['_1'].getItem('_' + str(i)))
    end_unpack = time.time() - unpack_begin
    print("Finished unpacking columns from zipped format in {} seconds.".format(end_unpack))
    # Crucial to not include column of columns
    metric_df = metric_df.drop('_1')
    return metric_df


def give_id_time(metric_df):
    """ Assign an id column to a spark df.

    :param metric_df: Spark df with metric.
    :return: A spark df with an id.
    """
    print("Starting to zip indices.")
    begin_zip = time.time()
    rdd_df = metric_df.rdd.zipWithIndex()
    end_zip = time.time() - begin_zip
    print("Completed zipping indices in {} seconds.".format(end_zip))

    print("Going from zipped rdd to df.")
    begin_df = time.time()
    metric_df = rdd_df.toDF()
    end_df = time.time() - begin_df
    print("Completed going from zipped rdd to df in {} seconds.".format(end_df))

    # Rename
    metric_df = metric_df.withColumnRenamed('_2', "id_key")
    metric_df = metric_df.withColumn('time', metric_df['_1'].getItem('time'))
    metric_df = metric_df.drop('_1')

    print("Metric looks like this: {}".format(metric_df.printSchema()))
    return metric_df


def give_all_metrics_id(metrics):
    """ Give all metrics an id to join with time.

    :param metrics: A dictionary containing spark data frames of ocean wave metrics.
    :return: A dictionary containing same ocean wave metrics with an id.
    """
    metrics_with_id = {}
    for key in metrics:
        metrics_with_id[key] = give_id(metrics[key])
    return metrics_with_id


def join_time_to_metrics(metrics_with_id, time_df):
    """ Give each wave metric a time index to query on.

    :param metrics_with_id: A dictionary of wave metrics with an id for joining.
    :param time_df: A data frame containing time indices.
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
    """ Write ocean wave metrics and coordinates to a data base.

    :param db_name: Name of data base to write to.
    :param metrics_with_time_index: A dictionary of ocean wave metrics, each data frame indexed by time.
    :param coordinates: A data frame with lat and long columns.
    :return: None.
    """
    print("Writing coordinates to db.")
    write_to_postgres(db_name, coordinates, "coordinates")
    print("Writing coordinates ended.")
    begin_all = time.time()
    for key in metrics_with_time_index:
        table_name = key
        data_frame = metrics_with_time_index[key]
        print("Writing {} to postgres.".format(table_name))
        begin_write = time.time()
        write_to_postgres(db_name, data_frame, table_name)
        end_write = time.time() - begin_write
        print("Writing {} to postgres ended in {} seconds.".format(table_name, end_write))
    end_all = time.time() - begin_all
    print("Data base writing process took {} seconds to complete.".format(end_all))


def run_four_year_block(start_year, end_year):
    """ Encapsulate pipe line ingestion and processing for four years.

    :param start_year: Year to start a four year block on. Minimum is 1979.
    :param end_year: Year to start a four year block on. Max is 2010.
    :return:
    """
    # Get relevant data frames
    # - time indices get called to s3 directly on driver
    time_df = get_time_index_hindcast(start_year)
    time_df = time_df.withColumnRenamed("0", "time")
    time_df = give_id_time(time_df)


    # - take coordinates from 1979 since coordinates won't change across years
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_1979.h5"
    coord_df = create_df_coords(s3_endpoint)
    coord_df = coord_df.withColumnRenamed("_1", "latitude")
    coord_df = coord_df.withColumnRenamed("_2", "longitude")

    # - get six most important wave metrics
    # - - significant_wave_height, omni_directional_wave_power, spectral_width,
    # - - maximum_energy_direction, energy_period, directionality_coefficient
    base_file_path = "hindcast_links/"
    metrics = turn_metrics_into_df(base_file_path, start_year, end_year)
    metrics_with_id = give_all_metrics_id(metrics)

    # Operation
    metrics_with_time_index = join_time_to_metrics(metrics_with_id, time_df)

    # Write to postgresql
    db_name = "hindcast"
    write_to_db(db_name, metrics_with_time_index, coord_df)


if __name__ == "__main__":
    # Spark setup
    sc = SparkContext(appName="Distribute reading of HDF5 files")
    spark = SparkSession(sc)

    # Reduce information printed on spark terminal
    sc.setLogLevel("ERROR")

    begin = time.time()
    start_year = 1979
    end_year = 1982
    run_four_year_block(start_year, end_year)
    end = time.time() - begin
    print("Entire process took {} seconds.".format(end))

    sc.stop()

