from ingest.ingest_s3 import call_s3_to_h5
from process.pandas_spark_converter import *
from database.spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id
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


def join_power_time(power_sp, time_sp):
    """ Create ID and join power and time.

    :param power_sp: Power spark data frame.
    :param time_sp: Time stamp spark data frame.
    :return: Power joined with time data frame.
    """
    power_sp = power_sp.withColumn("id_key", monotonically_increasing_id())
    time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
    power_sp = power_sp.join(time_sp, on="id_key")
    return power_sp


def write_to_db(db_name, power_sp, coord_sp, year):
    """ Coordinate writing tables to database.

    :param db_name: Data base name to write to in Postgres.
    :param power_sp: A power spark data frame.
    :param coord_sp: A coordinates spark data frame.
    :param year: Year ranging from 1979 to 2010.
    :return:
    """
    print("Started writing {} to db.".format("power table and coordinates"))
    write_to_postgres(db_name, power_sp, "power_" + year)
    write_to_postgres(db_name, coord_sp, "coords")
    print("Completed writing {} to db".format("power table"))


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
        power_sp = join_power_time(power_sp, time_sp)

        # Database writing
        db_name = "buoy_power"
        write_to_db(db_name, power_sp, coord_sp, year)

        end_single_year = time.time() - begin_single_year
        print("Single year took {} seconds to complete".format(end_single_year))

    end_all_years = time.time() - begin_all_years
    print("Buoy ingestion and processing completed.")
    print("Process took {} many seconds to complete".format(end_all_years))