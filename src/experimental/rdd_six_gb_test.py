"""
Module to encapsulate turning an h5 data set into a spark RDD.
- each file could be about ~6Gb
"""
import numpy as np
from ingest.ingest_s3 import call_s3_to_h5
from database.spark_to_db import write_to_postgres
from pyspark.sql import SparkSession
from pyspark.sql import Row

import time

spark = SparkSession.builder.appName("Ingest h5 files").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("Starting to get data set from s3.")

s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_2010.h5"
h5_file = call_s3_to_h5(s3_endpoint)

# Convert into rdd frame
# -- try a single column
overall_begin = time.time()
print("Start slicing")
coord_h5 = h5_file['energy_period'][0:2918,0:1000]
end_slice = time.time() - overall_begin
print("Slicing took {} seconds.".format(end_slice))

print("Got {} from h5 file".format(coord_h5))
print("Trying to turn into rdd")

coord_h5 = coord_h5.tolist()

rdd_time = time.time()
coord_rdd = sc.parallelize(coord_h5)
print("Finished turning into rdd")
end_rdd = time.time()
total_rdd = end_rdd - rdd_time
print("Job took {} seconds to complete".format(total_rdd))


# Convert into spark df
# -- key issue, going into spark df
print("Converting rdd into df")
df_time = time.time()

#row = Row("lat")
coord_df = coord_rdd.toDF()

df_end = time.time()
total_df = df_end - df_time
print("Finished turning into df {}".format(total_df))
print("Job took {} to complete")


# Write to TimescaleDB instance
db_name = "test_rdd_direct_read"
write_to_postgres(db_name, coord_df, "energy_period")
print("Completed writing {} to db".format(h5_file))
print("Completed writing of all data sets in h5 file.")
overall_end = time.time() - overall_begin
print("Entire process took {} seconds.".format(overall_end))