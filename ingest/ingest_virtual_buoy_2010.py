import s3fs
import h5py
# Simplify statement to import all
from pandas_spark_converter import *
from spark_to_db import write_to_postgres

print("Starting to process data sets in h5 file.")
s3 = s3fs.S3FileSystem()
h5_file = h5py.File(s3.open("s3://wpto-pds-us-wave/v1.0.0/virtual_buoy/US_virtual_buoy_2010.h5", "rb"))

dataset_keys = list(h5_file.keys())

for dataset_name in dataset_keys:
    # Errors encountered if directly reading in following data sets
    if dataset_name not in ["directional_wave_spectrum"]:
        dataset = h5_file[dataset_name]

        if dataset_name == "time_index":
            sp_data_frame = h5_time_to_pd_to_spark(dataset)
        elif dataset_name == "meta":
            sp_data_frame = h5_meta_to_pd_to_spark(dataset)
        else:
            sp_data_frame = h5_to_pd_to_spark(dataset)

        # Write to TimescaleDB instance
        db_name = "non_processed_virtual_buoy_2010"
        write_to_postgres(db_name, sp_data_frame, dataset_name)
        print("Completed writing {} to db".format(dataset))

print("Completed writing of all data sets in h5 file.")