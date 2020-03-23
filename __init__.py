"""
Initializing the SQL_PARAMS parameter to our json from our connections file.
"""
import json
from pathlib import Path
import os


# The init will attempt to create a dictionary of SQL Parameters that can be accessed through SQLConn. The below text
# provides an example, including explanations, of what should be present in the JSON file.
"""
{
    "credentials":
    {
        "<corresponds to type from db nick name>":
        {
            "username": "<mandatory string>",
            "password": "<mandatory string>"
        },
        .
        .
        .
    },
    "databases":
    {
        "<db nick name>":
        {
            "host": "<mandatory string>",
            "database": "<mandatory string>",
            "port": <mandatory int>,
            "type": "<mandatory string corresponding to python helper library for SQL engine connection>" 
            "username": "<optional string if not included must have corresponding username/password for this type>",
            "password": "<optional string if not included must have corresponding username/password for this type>"
        },
        .
        .
        .
    }
}
"""


# We load the connection parameters from a json file to keep passwords hidden.
# First set the search locations in order of priority (first is best).
valid_locations = {1: Path('/data', 'code', 'config', 'connconfig.json'),
                   2: Path(Path.home(), 'code', 'config', 'connconfig.json'),
                   3: Path(Path(__file__).resolve().parent, 'connconfig.json')}

# Loop through and load config files.
found_configs = {}
for key_ in valid_locations:
    try:
        with open(valid_locations[key_]) as fh:
            found_configs[key_] = json.load(fh)
    except:
        pass

# Break if none found.
if len(found_configs.keys()) == 0:
    raise RuntimeError('Not able to find the connconfig.json file for SQLConn')

MASTER_CREDS = {}
# Want to push credentials down from highest priority (lowest key value) file to lower levels. Sorted call for clarity.
for key_ in sorted(found_configs.keys()):
    config = found_configs[key_]
    if 'credentials' in config.keys():
        for type_ in config['credentials']:
            if type_ not in MASTER_CREDS.keys():
                uname_ = config['credentials'][type_]['username']
                pword_ = config['credentials'][type_]['password']

                if uname_ and pword_:
                    MASTER_CREDS[type_] = {'username': uname_, 'password': pword_}


if len(MASTER_CREDS.keys()) == 0:
    raise RuntimeError('No config file has valid credentials')
# Append overlap from highest priority (lowest key value) first.  Sorted call for clarity.
SQL_PARAMS = {}
for key_ in sorted(found_configs.keys()):
    # We depend upon having a section of json at the outer level called databases
    config = found_configs[key_]
    if 'databases' in config.keys():
        dbs = config['databases']
        for db in dbs:
            if db not in SQL_PARAMS:
                SQL_PARAMS[db] = dbs[db]
                # Now we have to check to make sure the username and password are present, if either one is not present
                # then we will replace both. If we get here, we depend upon there being a section titled credentials
                # in the outermost level of json. The keys in within credentials should be the same as the type present
                # in the database descriptor.
                if 'username' not in SQL_PARAMS[db].keys() or 'password' not in SQL_PARAMS[db].keys():
                    SQL_PARAMS[db]['username'] = MASTER_CREDS[SQL_PARAMS[db]['type']]['username']
                    SQL_PARAMS[db]['password'] = MASTER_CREDS[SQL_PARAMS[db]['type']]['password']
                    if SQL_PARAMS[db]['username'] is None or SQL_PARAMS[db]['password'] is None:
                        raise ConnectionError(f"The SQL connections do not have proper credentials, please copy the file "
                                              f"from {Path(Path(__file__).resolve().parent, 'connconfig.json')}\n"
                                              f"and place the file at /data/code/config/connconfig.json (Linux) or "
                                              f"C:\\data\\code\\config\\connconfig.json (Windows). Then update the\n"
                                              f"credentials section of the file to include the correct username/password "
                                              f"combinations. If the file is already there, then just update the\n"
                                              f"credentials and make sure nothings is null. Have a great day.")


from .sqlconn import SQLConn
from .sqlqueue import SQLQueue
from .sqlparams import *
