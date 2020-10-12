"""
Module to enable conversion from h5 data set to a spark df
"""

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ingest h5 files").getOrCreate()
sc = spark.sparkContext

# Reduce information printed on spark terminal
sc.setLogLevel("ERROR")


def h5_to_spark(data_set):
    """ Convert an h5 data set into a spark df.

    :param data_set: An h5 data set.
    :return: A spark df.
    """
    list_dataset = data_set.tolist()
    rdd_dataset = sc.parallelize(list_dataset)
    sp_dataset = rdd_dataset.toDF()
    return sp_dataset


def h5_to_pd_to_spark(data_set):
    """ Take an h5 dataset and convert into a pandas df, then spark df.
    Output a spark df.

    :param data_set: An h5 data set.
    :return:
    """
    print("Converting {} to pandas df".format(data_set))
    pd_data_frame = pd.DataFrame(np.array(data_set))
    print("Finished converting {} to pandas df".format(data_set))
    print("Converting pd {} to spark df".format(data_set))
    sp_data_frame = spark.createDataFrame(pd_data_frame)
    print("Finished converting {} to spark df".format(data_set))
    return sp_data_frame


def h5_time_to_pd_to_spark(data_set):
    """ Take in an h5 time stamped data set. Output a spark df.

    :param data_set:
    :return:
    """
    converted_time_np = [np.datetime64(entry) for entry in data_set]
    time_pd = pd.DataFrame(converted_time_np)
    sp_time = spark.createDataFrame(time_pd)
    return sp_time


def h5_meta_to_pd_to_spark(dataset):
    """ Custom method to transform bytes in string literals.

    :param dataset:
    :return:
    """
    meta_pd = pd.DataFrame(np.array(dataset))
    jurisdiction = meta_pd['jurisdiction']
    N = len(jurisdiction)
    for i in range(N):
        jurisdiction[i] = jurisdiction[i].decode("utf-8")
    meta_pd['jurisdiction'] = jurisdiction
    sp_meta = spark.createDataFrame(meta_pd)
    return sp_meta

