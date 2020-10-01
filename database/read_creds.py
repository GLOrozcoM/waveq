"""
Module to encapsulate reading credentials.
- DB roles and passwords
- IP addresses
"""

def read_db_creds(file_path):
    """ Read in creds from a file.

    :param file_path:
    :return:
    """
    # TODO clean up
    # -- assume single line holds a role and password
    properties = {}
    file = open(file_path, "r")
    line = file.readline()
    list_properties = line.strip().split(",")
    properties[list_properties[0]] = list_properties[1]
    properties[list_properties[2]] = list_properties[3]
    file.close()
    return properties

def read_ip_addresses(file_path):
    """ Read in IP addresses found at file_path.

    :param file_path:
    :return:
    """
    ip_addresses = {}
    with open(file_path, "r") as file:
        for line in file:
            key, val = line.split(",")
            val = val.replace("\n", "")
            ip_addresses[key] = val
    return ip_addresses
