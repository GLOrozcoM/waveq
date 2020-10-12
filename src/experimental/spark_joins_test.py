"""
Module to encapsulate joining of Spark data frames.
"""

import s3fs
import h5py
from pandas_spark_converter import *
from spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id

print("Starting to process data sets in h5 file.")
s3 = s3fs.S3FileSystem()
h5_file = h5py.File(s3.open("s3://wpto-pds-us-wave/v1.0.0/virtual_buoy/US_virtual_buoy_2010.h5", "rb"))

# Give significant wave
swh_sp = h5_to_pd_to_spark(h5_file['significant_wave_height'])
swh_sp = swh_sp.withColumn("id_key", monotonically_increasing_id())

time_sp = h5_time_to_pd_to_spark(h5_file['time_index'])

# Can't have same column in dataframe
time_sp = time_sp.withColumnRenamed("0", "Time")
time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())

swh_and_time = swh_sp.join(time_sp, on="id_key")
print(swh_and_time.count())

# Write to TimescaleDB instance
db_name = "non_processed_virtual_buoy_2010"
write_to_postgres(db_name, swh_and_time, "time_index_new")
print("Completed writing {} to db".format(swh_and_time))

print("Completed writing of data sets in h5 file.")