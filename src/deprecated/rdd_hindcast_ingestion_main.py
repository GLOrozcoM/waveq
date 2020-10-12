"""
Benchmark against:
- Module to encapsulate ingesting buoy data.
- Currently takes 5671 seconds (94 minutes) to complete.
- Single year takes 174 seconds to complete.
- Single location write out takes 3 seconds to complete.
"""


from process.pandas_spark_converter import *
from rdd_geo_buoy_ingestion_utility import *
import time



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


def make_location_datasets(coord_sp, metric_list, time_sp, year, metric_names):
    """ Given a year and relevant data sets, write out a power table for a single location.

    :param metric_list: A list containing metrics to join together.
    :param year: A year ranging from 1979 to 2010.
    :return: None
    """
    print("Starting to create geo tagged data sets")
    coords_sp_driver = coord_sp.collect()
    for location_index in range(0, 1000):
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
        db_name = "hindcast_full"
        write_to_db(db_name, geo_metrics_sp, year, location_index)

        print("Finished creating a single location based data set.")
        end_single_location = time.time() - begin_single_location
        print("Single location took {} seconds to complete".format(end_single_location))


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


def write_to_db(db_name, geo_metrics_sp, year, j):
    """ Write geographic buoy power data for a particular year to db.

    :param db_name: Data base name in Postgres to write to.
    :param geo_metrics_sp: Geographic power data.
    :param year: Year ranging from 1979 to 2010.
    :param j: Location index for data set.
    :return: None
    """
    print("Started writing {}th geo tagged data set to db.".format(j))
    begin = time.time()
    write_to_postgres(db_name, geo_metrics_sp, "geo_metrics_" + year)
    end = time.time() - begin
    print("Completed writing geo data set to db in {} seconds.".format(end))



if __name__ == "__main__":

    begin_all_years = time.time()

    # Data is divided into 32 separate files for 32 years (1979 - 2010)
    for i in range(0, 32):
        begin_single_year = time.time()
        year = str(1979 + i)

        # Ingestion and processing
        h5_file = get_s3_data_hindcast(year)
        energy_h5, swh_h5, time_h5, coord_h5, omni_direct_pwr_h5, \
        direct_coeff_h5, max_energy_direct_h5, spectral_width_h5 = extract_variables_hindcast(h5_file)
        vars = [energy_h5, swh_h5, omni_direct_pwr_h5, direct_coeff_h5, max_energy_direct_h5, spectral_width_h5]
        sliced_sets = slice_sets(vars)

        # TODO uncomment to include power calculation
        #sliced_swh_energy = [energy_h5, swh_h5]
        #power_h5 = calculate_power(sliced_swh_energy)

        metric_list = sliced_sets
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