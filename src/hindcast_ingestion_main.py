"""
Module to encapsulate getting data for first 1000 locations of wave hindcast
data.
Current times:
On average, slicing a single data set with 0:2918, 0:1000 takes 13 seconds.
A single year takes 100 seconds to complete.
"""

from ingest.ingest_s3 import call_s3_to_h5
from process.pandas_spark_converter import *
from database.spark_to_db import write_to_postgres
from pyspark.sql.functions import monotonically_increasing_id
import time

begin_all_years = time.time()


def get_s3_h5(year):
    """

    :param year:
    :return:
    """
    s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_" + year + ".h5"
    print("Starting to get data for year {}.".format(year))
    h5_file = call_s3_to_h5(s3_endpoint)
    print("Completed getting h5 file from s3.")
    return h5_file


def extract_vars(h5_file):
    """

    :param h5_file:
    :return:
    """
    energy_h5 = h5_file['energy_period']
    swh_h5 = h5_file['significant_wave_height']
    time_h5 = h5_file['time_index']
    coord_h5 = h5_file['coordinates']
    return energy_h5, swh_h5, time_h5, coord_h5


def slice_sets(vars_list):
    """ Slice all sets within the vars list passed in.

    :param energy_h5:
    :param swh_h5:
    :return:
    """
    sliced_vars = []
    y_range = [0,2918]
    x_range = [0,1000]
    for var in vars_list:
        single_sliced = slice_single_set(var, y_range, x_range)
        sliced_vars.append(single_sliced)
    return sliced_vars


def slice_single_set(h5_dataset, y_range, x_range):
    """ Perform a slice on a single h5 data set.

    :param h5_dataset:
    :param y_axis:
    :param x_axis
    :return:
    """
    y_begin = y_range[0]
    y_end = y_range[1]
    x_begin = x_range[0]
    x_end = x_range[1]
    print("Starting to slice data set.")
    begin = time.time()
    sliced_set = h5_dataset[y_begin:y_end, x_begin:x_end]
    end = time.time() - begin
    print("Finished slicing. It took {} seconds.".format(end))
    return sliced_set


def calculate_power(sliced_sets):
    """

    :param sliced_sets:
    :return:
    """
    print("--calculating power")
    # -- power =  0.5 * (swh)^2 * energy_period, formula for wattage power
    power_h5 = 0.5 * (sliced_sets[1] ** 2) * sliced_sets[0]
    print("--completed power")
    return power_h5


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


def convert_coord_time_spark_df(time_h5, coord_h5):
    """

    :return:
    """
    print("Converting time to spark df")
    time_sp = h5_time_to_pd_to_spark(time_h5)
    time_sp = time_sp.withColumnRenamed("0", "time")
    print("Completed time conversion to spark df")
    print("Converting ooordinates to spark df")
    coord_sp = h5_to_pd_to_spark(coord_h5)
    print("Completed coordinate conversion to spark df")
    return time_sp, coord_sp


def write_to_db(db_name, power_sp, coord_sp, year):
    """

    :return:
    """
    print("Started writing {} to db.".format("power table"))
    write_to_postgres(db_name, power_sp, "power_" + year)
    write_to_postgres(db_name, coord_sp, "coordinates")
    print("Completed writing {} to db".format("power table"))


def join_power_time(power_sp, time_sp):
    """

    :return:
    """
    power_sp = power_sp.withColumn("id_key", monotonically_increasing_id())
    time_sp = time_sp.withColumn("id_key", monotonically_increasing_id())
    power_sp = power_sp.join(time_sp, on="id_key")
    return power_sp


# Data is divided into 32 separate files for 32 years (1979 - 2010)
for i in range(0, 32):
    begin_single_year = time.time()
    year = str(1979 + i)

    h5_file = get_s3_h5(year)
    energy_h5, swh_h5, time_h5, coord_h5 = extract_vars(h5_file)
    vars = [energy_h5, swh_h5]
    sliced_sets = slice_sets(vars)
    power_h5 = calculate_power(sliced_sets)

    # Convert to spark df
    print("Converting power to spark df")
    power_sp = h5_to_pd_to_spark(power_h5)
    print("Completed power conversion to spark df")


    time_sp, coord_sp = convert_coord_time_spark_df(time_h5, coord_h5)
    power_sp = join_power_time(power_sp, time_sp)

    # Write to TimescaleDB instance
    db_name = "hindcast_test"
    write_to_db(db_name, power_sp, coord_sp, year)

    end_single_year = time.time() - begin_single_year
    print("Single year took {} seconds to complete".format(end_single_year))

end_all_years = time.time() - begin_all_years
print("Hindcast wave ingestion and processing completed")
print("Process took {} many seconds to complete".format(end_all_years))