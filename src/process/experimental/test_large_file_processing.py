# Simplify statement to import all
from ingest_s3 import get_s3_h5
from pandas_spark_converter import *
from spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id
import time

begin = time.time()

print("Starting to get h5 file from s3.")
s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_2010.h5"
h5_file = get_s3_h5(s3_endpoint)
print("Completed getting h5 file from s3.")

# -- power = (swh)^2 * energy_period * 0.5

## base sets
energy_h5 = h5_file['energy_period']
swh_h5 = h5_file['significant_wave_height']
time_h5 = h5_file['time_index']

## sliced sets
print("Starting to slice h5")
print("--slicing energy")
energy_slice_h5 = energy_h5[0:2918, 0:1000]
print("--completed energy")
print("--slicing swh")
swh_slice_h5 = swh_h5[0:2918, 0:1000]
print("--completed swh")
print("--calculating power")
power_h5 = 0.5 * (swh_slice_h5**2) * energy_slice_h5
print("--completed power")

# Convert to spark df
print("Converting power to spark df")
power_sp = h5_to_pd_to_spark(power_h5)
print("Completed power conversion to spark df")

print("Converting time to spark df")
time_sp = h5_time_to_pd_to_spark(time_h5)
time_sp = time_sp.withColumnRenamed("0", "time")
print("Completed time conversion to spark df")

# Perform join to time set
# -- create key to join on
power_sp = power_sp.withColumn("id_key", monotonically_increasing_id())
time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
power_sp = power_sp.join(time_sp, on="id_key")

# Write to TimescaleDB instance
print("Started writing {} to db.".format("power table"))
db_name = "power_test"
write_to_postgres(db_name, power_sp, "power")
print("Completed writing {} to db".format("power table"))
print("Single column completed")

end = time.time() - begin
print("Test completed")
print("Test took {} many seconds to complete".format(end))