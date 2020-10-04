"""
Module to encapsulate ingesting buoy data.
Currently takes 5671 seconds (94 minutes) to complete.
Single year takes 174 seconds to complete.
Single location write out takes 3 seconds to complete.
"""

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
    energy_h5 = h5_file['energy_period'][:]
    swh_h5 = h5_file['significant_wave_height'][:]
    omni_direct_pwr_h5 = h5_file['omni-directional_wave_power']
    direct_coeff_h5 = h5_file['directionality_coefficient']
    max_energy_direct_h5 = h5_file['maximum_energy_direction']
    spectral_width_h5 = h5_file['spectral_width']

    # Hourly, starting January 1st and going until December 31st
    time_h5 = h5_file['time_index']
    # Lat long format
    coord_h5 = h5_file['coordinates']
    return energy_h5, swh_h5, time_h5, coord_h5, omni_direct_pwr_h5, direct_coeff_h5, max_energy_direct_h5, spectral_width_h5


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


def convert_coord_time_spark_df(time_h5, coord_h5):
    """ Convert h5 data to spark df.

    :param time_h5: Time index in h5 format.
    :param coord_h5: Coordinates in h5 format.
    :return: Data sets converted to spark data frames.
    """
    print("Converting time to spark df")
    time_sp = h5_time_to_pd_to_spark(time_h5)
    time_sp = time_sp.withColumnRenamed("0", "time")
    print("Completed time conversion to spark df")

    print("Converting coordinates to spark df")
    coord_sp = h5_to_pd_to_spark(coord_h5)
    print("Completed coordinate conversion to spark df")

    return time_sp, coord_sp


def convert_metrics_spark_df(metric_list):
    """ Convert all metrics to a spark df.

    :param metric_list: A list with all metrics to convert to spark df.
    :return:
    """
    N = len(metric_list)
    converted_metrics = []
    for i in range(0, N):
        print("Converting {} to spark df".format(metric_list[i]))
        single_metric = h5_to_pd_to_spark(metric_list[i])
        print("Completed {} conversion to spark df".format(metric_list[i]))
        converted_metrics.append(single_metric)
    return converted_metrics


def write_to_db(db_name, geo_metrics_sp, year, j):
    """ Write geographic buoy power data for a particular year to db.

    :param db_name: Data base name in Postgres to write to.
    :param geo_metrics_sp: Geographic power data.
    :param year: Year ranging from 1979 to 2010.
    :param j: Location index for data set.
    :return: None
    """
    print("Started writing {}th geo tagged data set to db.".format(j))
    write_to_postgres(db_name, geo_metrics_sp, "geo_metrics_" + year)
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


def give_id(metric_sp, j):
    """ Assign an id column to a selected power spark df.

    :param metric_sp: Spark df with power metric.
    :param j: Location index of coordinate; 57 distinct locations exist.
    :return: A power spark df with an id.
    """
    single_column_metric = metric_sp.select(str(j))
    single_column_metric = single_column_metric.withColumn("id_key", monotonically_increasing_id())
    return single_column_metric


def rename_metrics(metric_list, metric_names, location_index):
    """ Give meaningful names to columns in metrics.

    :param metric_list: A list containing metrics as spark data frames.
    :param metric_names: Names for each metric. Order of metrics follows the metric list.
    :param location_index: Location indexed in table to rename.
    :return: Metric list with columns renamed.
    """
    N = len(metric_list)
    meaningful_metrics = []
    for i in range(0, N):
        metric = metric_list[i].withColumnRenamed(str(location_index), metric_names[i])
        meaningful_metrics.append(metric)
    return meaningful_metrics


def give_id_all_metrics(metric_list, j):
    """ Assign an id to all metrics.

    :param metric_list: List containing all metrics.
    :param j: Location index of coordinate; 57 distinct locations exist.
    :return: A list of metrics with id keys.
    """
    N = len(metric_list)
    id_metrics = []
    for i in range(0, N):
        single_id_metric = give_id(metric_list[i], j)
        id_metrics.append(single_id_metric)
    return id_metrics


def assign_coords(final_df, lat, long):
    """ Assign lat and long to a power column in spark.

    :param final_df: Spark column with power.
    :param lat: Latitude column.
    :param long: Longitude column.
    :param j: Location index of coordinate; 57 distinct locations exist.
    :return: Lat and long coordinates combined with power.
    """
    geo_df = final_df.withColumn("lat", lit(lat))
    geo_df = geo_df.withColumn("long", lit(long))
    return geo_df


def join_metrics(metric_list):
    """ Join all metrics into one table.

    :param metric_list: List containing metrics in spark df format.
    :return: A single table with all metrics.
    """
    # Use first metric to begin join process
    final_df = metric_list[0]
    N = len(metric_list)
    for i in range(1, N):
        final_df = final_df.join(metric_list[i], on="id_key")
    return final_df


def make_location_datasets(coord_sp, metric_list, time_sp, year, metric_names):
    """ Given a year and relevant data sets, write out a power table for a single location.

    :param metric_list: A list containing metrics to join together.
    :param year: A year ranging from 1979 to 2010.
    :return: None
    """
    print("Starting to create geo tagged data sets")
    coords_sp_driver = coord_sp.collect()
    for location_index in range(0, 57):
        begin_single_location = time.time()
        print("Start creating {}th location".format(location_index))

        # Combine all metrics
        id_metrics = give_id_all_metrics(metric_list, location_index)
        named_metrics = rename_metrics(id_metrics, metric_names, location_index)
        geo_metrics_sp = join_metrics(named_metrics)
        lat, long = access_lat_long(coords_sp_driver, location_index)
        geo_metrics_sp = assign_coords(geo_metrics_sp, lat, long)

        time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
        geo_metrics_sp = geo_metrics_sp.join(time_sp, on="id_key")

        # Database writing
        db_name = "geo_extracted_vars_buoy"
        write_to_db(db_name, geo_metrics_sp, year, location_index)

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
        energy_h5, swh_h5, time_h5, coord_h5, omni_direct_pwr_h5, \
        direct_coeff_h5, max_energy_direct_h5, spectral_width_h5 = extract_variables(h5_file)

        power_h5 = calculate_power(swh_h5, energy_h5)
        metric_list = [energy_h5, swh_h5, omni_direct_pwr_h5, direct_coeff_h5,
                       max_energy_direct_h5, spectral_width_h5, power_h5]
        metric_names = ["energy_period", "significant_wave_height", "omni_directional_power","directionality_coefficient",
                        "maximum_energy_directionality", "spectral_width", "power"]
        metrics_sp = convert_metrics_spark_df(metric_list)
        time_sp, coord_sp = convert_coord_time_spark_df(time_h5, coord_h5)

        # Attach coordinates to individual locations and write individual tables to db
        make_location_datasets(coord_sp, metrics_sp, time_sp, year, metric_names)

        end_single_year = time.time() - begin_single_year
        print("A single year took {} seconds to process".format(end_single_year))

    end_all_years = time.time() - begin_all_years
    print("Geographic focus of buoy ingestion and processing completed")
    print("Process took {} many seconds to complete".format(end_all_years))