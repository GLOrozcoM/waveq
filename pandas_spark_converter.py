"""
Module to enable conversion from h5 data set to a spark df
"""

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Ingest h5 files").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

def h5_to_pd_to_spark(dataset):
    """ Take an h5 dataset and convert into a pandas df, then spark df.
    Output a spark df.

    :param dataset: An h5 data set.
    :return:
    """
    print("Converting {} to pandas df".format(dataset))
    pd_data_frame = pd.DataFrame(np.array(dataset))
    print("Finished converting {} to pandas df".format(dataset))
    print("Converting pd {} to spark df".format(dataset))
    sp_data_frame = spark.createDataFrame(pd_data_frame)
    print("Finished converting {} to spark df".format(dataset))
    return sp_data_frame

def h5_time_to_pd_to_spark(dataset):
    """ Take in an h5 time stamped dataset. Output a spark df.

    :param dataset:
    :return:
    """
    time_np = np.array(dataset)
    # TODO optimize for loop using apply or similar
    converted_time_np = []
    for entry in time_np:
        converted_time_np.append(np.datetime64(entry))
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

