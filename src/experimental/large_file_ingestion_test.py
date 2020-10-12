# Simplify statement to import all
from ingest_s3 import get_s3_h5
from pandas_spark_converter import *
from spark_to_db import write_to_postgres

print("Starting to get h5 file from s3.")
s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_2010.h5"
h5_file = get_s3_h5(s3_endpoint)
print("Completed getting h5 file from s3.")

# Test drive a single data source block reading
# via indexing

energy_h5 = h5_file['energy_period']

# Will have to specify grid size and time size
print("Start processing of a single data set")
block_size = 1000
window = 0
# Hard code 2 for test purposes
for i in range(0, 200):
    # TODO generalize column counts
    print("The window:", window)
    print(block_size)
    subset = energy_h5[0:2918, window:block_size]
    print(subset)
    energy_sp = h5_to_pd_to_spark(subset)

    # Write to TimescaleDB instance
    print("Started writing {} to db.".format(energy_h5))
    db_name = "test_energy"
    write_to_postgres(db_name, energy_sp, "energy")
    print("Completed writing {} to db".format(energy_h5))
    print("Single column completed")

    # Coordinate the moving window for regional count
    window += block_size
    block_size += 1000


print("Test completed")