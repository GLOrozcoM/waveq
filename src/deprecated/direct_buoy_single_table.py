"""
Benchmark against:
- Module to encapsulate ingesting buoy data.
- Currently takes 5671 seconds (94 minutes) to complete.
- Single year takes 174 seconds to complete.
- Single location write out takes 3 seconds to complete.
"""


from process.pandas_spark_converter import *
from direct_ingestion_utility import *
import time


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
        db_name = "buoy_full"
        write_to_db(db_name, geo_metrics_sp, year, location_index)

        print("Finished creating a single location based data set.")
        end_single_location = time.time() - begin_single_location
        print("Single location took {} seconds to complete".format(end_single_location))



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
        h5_file = get_s3_data(year)
        energy_h5, swh_h5, time_h5, coord_h5, omni_direct_pwr_h5, \
        direct_coeff_h5, max_energy_direct_h5, spectral_width_h5 = extract_variables(h5_file)




        power_h5 = calculate_power(swh_h5, energy_h5)
        metric_list = [energy_h5, swh_h5, omni_direct_pwr_h5, direct_coeff_h5,
                       max_energy_direct_h5, spectral_width_h5, power_h5]
        metric_names = ["energy_period", "significant_wave_height", "omni_directional_power","directionality_coefficient",
                        "maximum_energy_directionality", "spectral_width", "power"]
        metrics_sp = convert_metrics_spark_df(metric_list)

        # Final result, a list of metrics_sp

        time_sp, coord_sp = convert_coord_time_spark_df(time_h5, coord_h5)

        # Attach coordinates to individual locations and write individual tables to db
        make_location_datasets(coord_sp, metrics_sp, time_sp, year, metric_names)

        end_single_year = time.time() - begin_single_year
        print("A single year took {} seconds to process".format(end_single_year))

    end_all_years = time.time() - begin_all_years
    print("Geographic focus of buoy ingestion and processing completed")
    print("Process took {} many seconds to complete".format(end_all_years))