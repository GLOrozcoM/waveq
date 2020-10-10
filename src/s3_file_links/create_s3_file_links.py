"""
This module encapsulate creating csv files containing s3 links to h5 ocean wave data.
"""

import csv


def make_row_hindcast(year, data_set):
    """ Create a single row to write out in csv file containing s3 file links.

    :param year: Year for data (starting at 1979 and ending 2010).
    :param data_set: Variable of interest (e.g. significant_wave_height)
    :return: A list to be written as a row in csv file.
    """
    s3_link = "s3://wpto-pds-us-wave/v1.0.0/US_wave_" + year + ".h5"
    # Will take 2918 time points (entire three hour resolution for a single year)
    slice_time = 2918
    # Will take 1000 locations in the west coast
    slice_location = 1000
    return [s3_link, data_set, slice_time, slice_location]


def make_rows_hindcast(start_year, data_set):
    """ Create rows to csv file that will contain all s3 file links. Creates four
    rows per csv file -> match the number of nodes in Spark cluster.

    Note this is for hindcast data only.

    :param start_year: Year to start writing data sets for.
    :param data_set: Variable of interest in data set.
    :return: A two dimensional list containing all rows.
    """
    rows = []
    for offset in range(4):
        curr_year = str(start_year + offset)
        single_row = make_row_hindcast(curr_year, data_set)
        rows.append(single_row)
    return rows


def make_row_buoy(year, data_set):
    """ Create a single row to write out in csv file containing s3 file links.

    :param year: Year for data (starting at 1979 and ending 2010).
    :param data_set: Variable of interest (e.g. significant_wave_height)
    :return: A list to be written as a row in csv file.
    """
    s3_link = "s3://wpto-pds-us-wave/v1.0.0/virtual_buoy/US_virtual_buoy_" + year + ".h5"
    return [s3_link, data_set]


def make_rows_buoy(start_year, data_set):
    """ Create rows to csv file that will contain all s3 file links. Creates four
    rows per csv file -> match the number of nodes in Spark cluster.

    Note this is for buoy data only.

    :param start_year: Year to start writing data sets for.
    :param data_set: Variable of interest in data set.
    :return: A two dimensional list containing all rows.
    """
    rows = []
    for offset in range(4):
        curr_year = str(start_year + offset)
        single_row = make_row_buoy(curr_year, data_set)
        rows.append(single_row)
    return rows


def write_four_year_block_hindcast(start_year):
    """ Create a csv file to contain all s3 file links.

    :param start_year: Year to start writing data for.
    :return: None.
    """
    data_sets = ['coordinates', 'directionality_coefficient', 'energy_period', 'maximum_energy_direction',
                 'omni-directional_wave_power', 'significant_wave_height', 'spectral_width', 'time_index']

    for single_set in data_sets:
        rows = make_rows_hindcast(start_year, single_set)
        file_name = "src/s3_file_links/hindcast_links/" + single_set + str(start_year) + \
                    "_to_" + str(start_year + 3) + ".csv"

        f = open(file_name, "w")
        with f:
            writer = csv.writer(f)
            writer.writerows(rows)


def write_four_year_block_buoy(start_year):
    """ Create a csv file to contain all s3 file links.

    :param start_year: Year to start writing data for.
    :return: None.
    """
    data_sets = ['coordinates', 'directionality_coefficient', 'energy_period', 'maximum_energy_direction',
                 'omni-directional_wave_power', 'significant_wave_height', 'spectral_width', 'time_index']

    for single_set in data_sets:
        rows = make_rows_hindcast(start_year, single_set)
        file_name = "src/s3_file_links/buoy_links/" + single_set + str(start_year) + "_to_" + str(start_year + 3) + ".csv"

        f = open(file_name, "w")
        with f:
            writer = csv.writer(f)
            writer.writerows(rows)


if __name__ == "__main__":
    # Will write csv files up to the year 2009
    start_year = 1979
    for cycle in range(10):
        write_four_year_block_hindcast(start_year)
        write_four_year_block_buoy(start_year)
        start_year += 3