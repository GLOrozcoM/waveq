from process.pandas_spark_converter import *
import time
from buoy_ingestion_utility import *


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