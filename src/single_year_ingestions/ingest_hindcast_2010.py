# Simplify statement to import all
from ingest_s3 import get_s3_h5
from pandas_spark_converter import *
from spark_to_db import write_to_postgres


print("Starting to get data set from s3.")
s3_endpoint = "s3://wpto-pds-us-wave/v1.0.0/US_wave_2010.h5"
h5_file = get_s3_h5(s3_endpoint)
print("Completed getting data from s3.")

dataset_keys = list(h5_file.keys())

for dataset_name in dataset_keys:
    # Errors encountered if operating directly on directional wave spectrum
    if dataset_name not in ["directional_wave_spectrum"]:
        dataset = h5_file[dataset_name]
        if dataset_name == "time_index":
            sp_data_frame = h5_time_to_pd_to_spark(dataset)
        elif dataset_name == "meta":
            sp_data_frame = h5_meta_to_pd_to_spark(dataset)
        else:
            sp_data_frame = h5_to_pd_to_spark(dataset)

        # Write to TimescaleDB instance
        db_name = "ingest_hindcast_2010"
        write_to_postgres(db_name, sp_data_frame, dataset_name)
        print("Completed writing {} to db".format(dataset))

print("Completed writing of all data sets in h5 file.")