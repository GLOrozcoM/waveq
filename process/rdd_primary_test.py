"""
Module to encapsulate turning an h5 data set into a spark RDD.
- each file could be about ~90Gb
"""

from ingest_s3 import get_s3_h5
from spark_to_db import write_to_postgres
from pyspark.sql import SparkSession

import time

spark = SparkSession.builder.appName("Ingest h5 files").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("Starting to get data set from s3.")

s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_2010.h5"
h5_file = get_s3_h5(s3_endpoint)

# Convert into rdd frame
coord_h5 = h5_file['coordinates']
print("Got {} from h5 file".format(coord_h5))
print("Trying to turn into rdd")
rdd_time = time.time()
coord_rdd = sc.parallelize(coord_h5)
print("Finished turning into rdd")
end_rdd = time.time()
total_rdd = end_rdd - rdd_time
print("Job took {} to complete".format(total_rdd))

# Convert into spark df
print("Converting rdd into df")
df_time = time.time()
coord_df = coord_rdd.toDF()
df_end = time.time()
total_df = df_end - df_time
print("Finished turning into df {}".format(total_df))
print("Job took {} to complete")


# Write to TimescaleDB instance
db_name = "ingest_hindcast_2010"
write_to_postgres(db_name, coord_df, "coord_df")
print("Completed writing {} to db".format(h5_file))

print("Completed writing of all data sets in h5 file.")