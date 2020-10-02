# Simplify statement to import all
from ingest.ingest_s3 import get_s3_h5
from process.pandas_spark_converter import *
from database.spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
import time

begin_all_years = time.time()

# TODO modularize components
# Data is divided into 32 separate files for 32 years (1979 - 2010)
for i in range(0, 32):
    year = str(1979 + i)

    begin_single_year = time.time()
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/virtual_buoy/US_virtual_buoy_" + year + ".h5"
    print("Starting to get data for year {}.".format(year))
    h5_file = get_s3_h5(s3_endpoint)
    print("Completed getting h5 file from s3.")

    # base sets
    # -- extract data with [:] to perform calculation later
    energy_h5 = h5_file['energy_period'][:]
    swh_h5 = h5_file['significant_wave_height'][:]
    time_h5 = h5_file['time_index']
    coord_h5 = h5_file['coordinates']

    # power calculation
    print("--calculating power")
    # -- power =  0.5 * (swh)^2 * energy_period, formula for wattage power
    power_h5 = 0.5 * (swh_h5**2) * energy_h5
    print("--completed power")

    # Convert to spark df
    print("Converting power to spark df")
    power_sp = h5_to_pd_to_spark(power_h5)
    print("Completed power conversion to spark df")

    print("Converting time to spark df")
    time_sp = h5_time_to_pd_to_spark(time_h5)
    time_sp = time_sp.withColumnRenamed("0", "time")
    print("Completed time conversion to spark df")

    print("Converting coordinates to spark df")
    coord_sp = h5_to_pd_to_spark(coord_h5)
    print("Completed coordinate conversion to spark df")

    # Perform join to time set
    # -- create key to join on
    power_sp = power_sp.withColumn("id_key", monotonically_increasing_id())
    time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
    power_sp = power_sp.join(time_sp, on="id_key")

    # Attach coordinates to individual locations and write individual
    # tables out
    # -- 57 distinct locations
    print("Starting to create geo tagged data sets")
    coords_sp_driver = coord_sp.collect()
    for i in range(0, 57):
        print("Create a geo data set")
        # -- lat is "0", long is "1"
        lat = coords_sp_driver[i]["0"]
        long = coords_sp_driver[i]["1"]
        single_column_power = power_sp.select(str(i))
        single_column_power = single_column_power.withColumn("id_key", monotonically_increasing_id())
        power_geo_sp = single_column_power.withColumn("lat", lit(lat))
        power_geo_sp = power_geo_sp.withColumn("long", lit(long))
        power_geo_sp = power_geo_sp.withColumnRenamed(str(i), "power")
        power_geo_sp = power_geo_sp.join(time_sp, on="id_key")

        print("Finished creating a geo dataset")

        # Write to TimescaleDB instance
        print("Started writing {}th geo tagged data set to db.".format(i))
        db_name = "buoy_power"
        write_to_postgres(db_name, power_geo_sp, "geo_power_" + year + "_loc" + str(i))
        print("Completed writing geo data set to db.")

    # Write to TimescaleDB instance
    print("Started writing {} to db.".format("power tables"))
    db_name = "buoy_power"
    write_to_postgres(db_name, power_sp, "power_" + year)
    write_to_postgres(db_name, coord_sp, "coords")
    print("Completed writing {} to db".format("power table"))
    end_single_year = time.time() - begin_single_year
    print("Single year took {} seconds to complete".format(end_single_year))

end_all_years = time.time() - begin_all_years
print("Buoy ingestion and processing completed")
print("Process took {} many seconds to complete".format(end_all_years))