"""
This module encapsulates getting all buoy data and writing to a database where
each table is structured by location and year.

DB: buoy_multi_table

- Currently takes 234 minutes (3.9 hours) to complete.
"""


from process.pandas_spark_converter import *
from buoy_ingestion_utility import *
import time


def give_id_og(metric_sp, location_index):
    """ Assign an id column to a selected spark df.

    :param metric_sp: Spark df with power metric.
    :param location_index: Location index of coordinate; 57 distinct locations exist.
    :return: A power spark df with an id.
    """
    single_column_metric = metric_sp.select("_" + str(location_index + 1))
    single_column_metric = zip_to_id(single_column_metric, location_index)
    return single_column_metric


def zip_to_id(metric_df, location_index):
    """ Assign an id column to a spark df.

    :param metric_df: Spark df with ocean metric.
    :param location_index: Locational index of coordinate; 57 distinct locations exist.
    :return: A power spark df with an id.
    """
    rdd_df = metric_df.rdd.zipWithIndex()
    metric_df = rdd_df.toDF()

    # Rename index
    metric_df = metric_df.withColumnRenamed('_2', "id_key")
    metric_df = metric_df.withColumn('_' + str(location_index + 1), metric_df['_1'].getItem('_' + str(location_index + 1)))

    # Crucial to not include column of columns
    metric_df = metric_df.drop('_1')
    return metric_df


def give_id_all_metrics(metric_list, location_index):
    """ Assign an id to all metrics.

    :param metric_list: List containing all metrics.
    :param location_index: Location index of coordinate; 57 distinct locations exist.
    :return: A list of metrics with id keys.
    """
    N = len(metric_list)
    id_metrics = []
    for i in range(0, N):
        single_id_metric = give_id_og(metric_list[i], location_index)
        id_metrics.append(single_id_metric)
    return id_metrics


def give_id_time(metric_df):
    """ Assign an id column to a spark df.

    :param metric_df: Spark df with metric.
    :return: A spark df with an id.
    """
    begin_zip = time.time()
    rdd_df = metric_df.rdd.zipWithIndex()
    end_zip = time.time() - begin_zip

    begin_df = time.time()
    metric_df = rdd_df.toDF()
    end_df = time.time() - begin_df

    # Rename
    metric_df = metric_df.withColumnRenamed('_2', "id_key")
    metric_df = metric_df.withColumn('time', metric_df['_1'].getItem('time'))
    metric_df = metric_df.drop('_1')
    return metric_df


def make_location_datasets(coord_sp, metric_list, time_sp, year, metric_names):
    """ Given a year and relevant data sets, write out a power table for a single location.

    :param metric_list: A list containing metrics to join together.
    :param year: A year ranging from 1979 to 2010.
    :return: None
    """
    coords_sp_driver = coord_sp.collect()
    for location_index in range(0, 57):
        begin_single_location = time.time()

        # Combine all metrics
        id_metrics = give_id_all_metrics(metric_list, location_index)
        named_metrics = rename_metrics(id_metrics, metric_names, location_index)
        geo_metrics_sp = join_metrics(named_metrics)
        lat, long = access_lat_long(coords_sp_driver, location_index)
        geo_metrics_sp = assign_coords(geo_metrics_sp, lat, long)


        time_sp = give_id_time(time_sp)
        geo_metrics_sp = geo_metrics_sp.join(time_sp, on="id_key")

        # Database writing
        db_name = "buoy_multi_table"
        write_to_db(db_name, geo_metrics_sp, year, location_index)

        end_single_location = time.time() - begin_single_location


def write_to_db(db_name, geo_metrics_sp, year, j):
    """ Write geographic buoy power data for a particular year to db.

    :param db_name: Data base name in Postgres to write to.
    :param geo_metrics_sp: Geographic power data.
    :param year: Year ranging from 1979 to 2010.
    :param j: Location index for data set.
    :return: None
    """
    begin = time.time()
    write_to_postgres(db_name, geo_metrics_sp, "wave_metrics_" + year + "_loc" + str(j))
    end = time.time() - begin



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

    end_all_years = time.time() - begin_all_years