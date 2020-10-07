"""
Module to encapsulate writing to PostgresDB (TimescaleDB)
"""
import sys
sys.path.insert(0, '~/home/ubuntu/waveq/database/')

from database.read_creds import *

def write_to_postgres(db_name, sp_data_frame, dataset_name):
    """ Write spark data frame to table in Postgres db.

    :param db_name: The name of the database to write to.
    :param sp_data_frame: A spark data frame.
    :param dataset_name: The original h5 data set to be converted to a spark df.
    :return:
    """
    # For security, IP addresses are not displayed in code.
    # -- place the master IP address in the top line of .ip_addresses.txt
    ip = read_ip_addresses("waveq/credentials/.ip_addresses.txt")['Master']
    url = "jdbc:postgresql://" + ip + "/" + db_name
    table_name = dataset_name.replace("-", "_")
    mode = "append"

    # For security, db credentials are not displayed in code.
    # -- place name of role and password for role in a single line in .db_creds.txt
    db_creds_file = "waveq/credentials/.db_creds.txt"
    properties = read_db_creds(db_creds_file)

    sp_data_frame.write.jdbc(url=url, table=table_name, mode=mode, properties=properties) \
                .option("createTableColumnTypes",
                        "id_key bigint, energy_period numeric, significant_wave_height numeric, omni_directional_power numeric,"
                        "directionality_coefficient numeric, maximum_energy_directionality numeric, spectral_width numeric,"
                        "power numeric, lat numeric, long numeric, time timestamp")
    print("Write to db ended for {}".format(dataset_name))