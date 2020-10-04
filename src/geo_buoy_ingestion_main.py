from ingest.ingest_s3 import call_s3_to_h5
from process.pandas_spark_converter import *
from database.spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit
import time


def get_s3_data(year):
    """ Interact with s3 to acquire wave data h5 files.

    :param year: String ranging from 1979 to 2010.
    :return: An h5 file containing wave data for a single year.
    """
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/virtual_buoy/US_virtual_buoy_" + year + ".h5"
    print("Starting to get data for year {}.".format(year))
    h5_file = call_s3_to_h5(s3_endpoint)
    print("Completed getting h5 file from s3.")
    return h5_file


def extract_variables(h5_file):
    """ Get variables of interest from h5 file.

    :param h5_file: An h5 file of wave data from s3 endpoint of the form
    "s3://wpto-pds-us-wave/v1.0.0/virtual_buoy/US_virtual_buoy_" + year + ".h5"
    :return:
    """
    # Extract actual data with [:] to perform calculation later
    energy_h5 = h5_file['energy_period'][:]
    swh_h5 = h5_file['significant_wave_height'][:]
    time_h5 = h5_file['time_index']
    # Lat long format
    coord_h5 = h5_file['coordinates']
    return energy_h5, swh_h5, time_h5, coord_h5


def calculate_power(swh_h5, energy_h5):
    """ Perform power calculation for expected wattage.

    :param swh_h5: Significant wave height.
    :param energy_h5: Energy period of wave.
    :return: Power in h5 file format.
    """
    print("--calculating power")
    # ormula for wattage power
    power_h5 = 0.5 * (swh_h5 ** 2) * energy_h5
    print("--completed power")
    return power_h5


def convert_spark_df(power_h5, time_h5, coord_h5):
    """ Convert h5 data to spark df.

    :param power_h5: Power in h5 format.
    :param time_h5: Time index in h5 format.
    :param coord_h5: Coordinates in h5 format.
    :return: Data sets converted to spark data frames.
    """
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

    return power_sp, time_sp, coord_sp


def write_to_db(db_name, power_geo_sp, year, j):
    """ Write geographic buoy power data for a particular year to db.

    :param db_name: Data base name in Postgres to write to.
    :param power_geo_sp: Geographic power data.
    :param year: Year ranging from 1979 to 2010.
    :param j: Location index for data set.
    :return: None
    """
    print("Started writing {}th geo tagged data set to db.".format(j))
    write_to_postgres(db_name, power_geo_sp, "geo_power_" + year + "_loc" + str(j))
    print("Completed writing geo data set to db.")


def access_lat_long(coords_sp_driver, j):
    """ Get the lat and long coordinates.

    :param coords_sp_driver: Coordinate spark data frame on driver.
    :param j: Location index of coordinate; 57 distinct locations exist.
    :return: lat and long columns of spark df.
    """
    lat = coords_sp_driver[j]["0"]
    long = coords_sp_driver[j]["1"]
    return lat, long


def give_id(power_sp, j):
    """ Assign an id column to a selected power spark df.

    :param power_sp: Spark df with power metric.
    :param j: Location index of coordinate; 57 distinct locations exist.
    :return: A power spark df with an id.
    """
    single_column_power = power_sp.select(str(j))
    single_column_power = single_column_power.withColumn("id_key", monotonically_increasing_id())
    return single_column_power


def assign_coords(single_column_power, lat, long, j):
    """ Assign lat and long to a power column in spark.

    :param single_column_power: Spark column with power.
    :param lat: Latitude column.
    :param long: Longitude column.
    :param j: Location index of coordinate; 57 distinct locations exist.
    :return: Lat and long coordinates combined with power.
    """
    power_geo_sp = single_column_power.withColumn("lat", lit(lat))
    power_geo_sp = power_geo_sp.withColumn("long", lit(long))
    power_geo_sp = power_geo_sp.withColumnRenamed(str(j), "power")
    return power_geo_sp


def make_location_datasets(coord_sp, power_sp, time_sp, year):
    """ Given a year and relevant data sets, write out a power table for a single location.

    :param j: Location index.
    :param coord_sp: A spark data frame of lat long coordinstes.
    :param power_sp: A spark data frame of power.
    :param time_sp: A spark data frame of time.
    :param year: A year ranging from 1979 to 2010.
    :return: None
    """
    print("Starting to create geo tagged data sets")
    coords_sp_driver = coord_sp.collect()
    for j in range(0, 57):
        begin_single_location = time.time()
        print("Start creating a single location based data set.")

        # Get lat longs and join with power data set
        lat, long = access_lat_long(coords_sp_driver, j)
        single_column_power = give_id(power_sp, j)
        power_geo_sp = assign_coords(single_column_power, lat, long, j)
        time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
        power_geo_sp = power_geo_sp.join(time_sp, on="id_key")

        # Database writing
        db_name = "geo_buoy_power"
        write_to_db(db_name, power_geo_sp, year, j)

        print("Finished creating a single location based data set.")
        end_single_location = time.time() - begin_single_location
        print("Single location took {} seconds to complete".format(end_single_location))


if __name__ == "__main__":

    begin_all_years = time.time()

    # Data is divided into 32 separate files for 32 years (1979 - 2010)
    for i in range(0, 32):
        begin_single_year = time.time()
        year = str(1979 + i)

        # Ingestion and processing
        h5_file = get_s3_data(year)
        energy_h5, swh_h5, time_h5, coord_h5 = extract_variables(h5_file)
        power_h5 = calculate_power(swh_h5, energy_h5)
        power_sp, time_sp, coord_sp = convert_spark_df(power_h5, time_h5, coord_h5)

        # Attach coordinates to individual locations and write individual tables to db
        make_location_datasets(coord_sp, power_sp, time_sp, year)

        end_single_year = time.time() - begin_single_year
        print("A single year took {} seconds to process".format(end_single_year))

    end_all_years = time.time() - begin_all_years
    print("Geographic focus of buoy ingestion and processing completed")
    print("Process took {} many seconds to complete".format(end_all_years))