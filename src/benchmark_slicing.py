"""
Module to test optimal slicing dimensions of h5 data sets before being processed.
"""

from ingest.ingest_s3 import call_s3_to_h5
import time
import os
import numpy as np

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
    with open("/home/ubuntu/waveq/src/logs/slice_log.txt", "a") as f:
        f.write("Slice of y_range (time stamp): {} and x_range (locations): {} took {} seconds.\n"\
                .format(y_range, x_range, end))
    return sliced_set


def slice_sets(vars_list):
    """ Slice all sets within the vars list passed in.

    :param energy_h5:
    :param swh_h5:
    :return:
    """
    sliced_vars = []
    y_range = [0,2918]
    x_range = [0,1600]
    for var in vars_list:
        single_sliced = slice_single_set(var, y_range, x_range)
        sliced_vars.append(single_sliced)
    return sliced_vars


year = str(1979)
h5_file = get_s3_h5(year)
energy_h5, swh_h5, time_h5, coord_h5 = extract_vars(h5_file)
vars = [energy_h5, swh_h5]
sliced_sets = slice_sets(vars)
