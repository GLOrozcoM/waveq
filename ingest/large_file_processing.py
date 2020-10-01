# Simplify statement to import all
from ingest_s3 import get_s3_h5
from pandas_spark_converter import *
from spark_to_db import write_to_postgres

print("Starting to get h5 file from s3.")
s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_2010.h5"
h5_file = get_s3_h5(s3_endpoint)
print("Completed getting h5 file from s3.")

# -- power = (swh)^2 * energy_period * 0.5

## base sets
energy_h5 = h5_file['energy_period']
swh_h5 = h5_file['significant_wave_height']

## sliced sets
print("Starting to slice h5")
print("--slicing energy")
energy_slice_h5 = energy_h5[0:2918, 0:500]
print("--completed energy")
print("--slicing swh")
swh_slice_h5 = swh_h5[0:2918, 0:500]
print("--completed swh")
print("--calculating power")
power_h5 = 0.5 * (swh_slice_h5**2) * energy_slice_h5
print("--completed power")

# Convert to spark df
print("Converting to spark df")
power_sp = h5_to_pd_to_spark(power_h5)
print("Completed conversion to spark df")

# Write to TimescaleDB instance
print("Started writing {} to db.".format("power table"))
db_name = "power_test"
write_to_postgres(db_name, power_sp, "power")
print("Completed writing {} to db".format("power table"))
print("Single column completed")


print("Test completed")