"""
Module to encapsulate writing to Postgres db
"""
from read_creds import *

def write_to_postgres(db_name, sp_data_frame, dataset_name):
    """ Write spark data frame to table in Postgres db.

    :param db_name:
    :param sp_data_frame:
    :param dataset_name:
    :return:
    """
    ip = read_ip_addresses("waveq/.ip_addresses.txt")['Master']
    url = "jdbc:postgresql://" + ip + "/" + db_name
    table_name = dataset_name.replace("-", "_")
    mode = "overwrite"
    db_creds_file = "waveq/.db_creds.txt"
    properties = read_db_creds(db_creds_file)
    sp_data_frame.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
    print("Write to db ended for {}".format(dataset_name))