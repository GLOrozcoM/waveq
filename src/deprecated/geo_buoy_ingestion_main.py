"""
Module to encapsulate ingesting buoy data.
Currently takes 5671 seconds (94 minutes) to complete.
Single year takes 174 seconds to complete.
Single location write out takes 3 seconds to complete.
"""


from process.pandas_spark_converter import *
from geo_buoy_ingestion_utility import *
import time

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