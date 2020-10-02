"""
Module to encapsulate reading credentials.
- DB roles and passwords
- IP addresses
"""


def read_db_creds(file_path):
    """ Read in creds from a file.

    :param file_path: String point to the file.
    :return: A dictionary containing the role as key and role password as value.
    """
    # -- assume single line holds a role and password
    role_creds = {}
    file = open(file_path, "r")
    line = file.readline()
    list_properties = line.strip().split(",")
    role_creds[list_properties[0]] = list_properties[1]
    role_creds[list_properties[2]] = list_properties[3]
    file.close()
    return role_creds


def read_ip_addresses(file_path):
    """ Read in IP addresses found at file_path.

    :param file_path: String point to the file.
    :return: A dictionary containing role of machine as key and IP address as value.
    """
    ip_addresses = {}
    with open(file_path, "r") as file:
        for line in file:
            key, val = line.split(",")
            val = val.replace("\n", "")
            ip_addresses[key] = val
    return ip_addresses
