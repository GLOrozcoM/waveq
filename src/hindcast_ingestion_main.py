# Simplify statement to import all
from ingest.ingest_s3 import get_s3_h5
from process.pandas_spark_converter import *
from database.spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id
import time

begin_all_years = time.time()

# TODO modularize components
# Data is divided into 32 separate files for 32 years (1979 - 2010)
for i in range(0, 32):
    year = str(1979 + i)

    begin_single_year = time.time()
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_" + year + ".h5"
    print("Starting to get data for year {}.".format(year))
    h5_file = get_s3_h5(s3_endpoint)
    print("Completed getting h5 file from s3.")

    ## base sets
    energy_h5 = h5_file['energy_period']
    swh_h5 = h5_file['significant_wave_height']
    time_h5 = h5_file['time_index']
    coord_h5 = h5_file['coordinates']

    ## sliced sets
    print("Starting to slice h5")
    print("--slicing energy")
    energy_slice_h5 = energy_h5[0:2918, 0:1000]
    print("--completed energy")
    print("--slicing swh")
    swh_slice_h5 = swh_h5[0:2918, 0:1000]
    print("--completed swh")
    print("--calculating power")
    # -- power =  0.5 * (swh)^2 * energy_period, formula for wattage power
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

    print("Converting ooordinates to spark df")
    coord_sp = h5_to_pd_to_spark(coord_h5)
    print("Completed coordinate conversion to spark df")

    # Perform join to time set
    # -- create key to join on
    power_sp = power_sp.withColumn("id_key", monotonically_increasing_id())
    time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
    power_sp = power_sp.join(time_sp, on="id_key")

    # Write to TimescaleDB instance
    print("Started writing {} to db.".format("power table"))
    db_name = "wave_hindcast_power_test"
    write_to_postgres(db_name, power_sp, "power_" + year)
    write_to_postgres(db_name, coord_sp, "coordinates")
    print("Completed writing {} to db".format("power table"))
    end_single_year = time.time() - begin_single_year
    print("Single year took {} seconds to complete".format(end_single_year))

end_all_years = time.time() - begin_all_years
print("Hindcast wave ingestion and processing completed")
print("Process took {} many seconds to complete".format(end_all_years))