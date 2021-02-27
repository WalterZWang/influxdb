# -*- coding: utf-8 -*-
"""
Grant access to demo data based on access.config.

"""

import csv
import os
import stat

def get_access(source):
    '''Get access to data source.

    Parameters
    ----------
    source : string
        Name of data source.

    Returns
    -------
    args : list
        Line of access file corresponding to data source name.

    '''
    # Get local directory
    path = os.path.dirname(os.path.abspath(__file__))
    # Get access data
    try:
        # Check permissions
        # Python2: &0777, In Ubuntu version, is '0600', was revised to '0666' in the windows version
        # Python3: &777, '0o400'
        if oct(os.stat(path+'/access.config')[stat.ST_MODE] & 777) != '0o400':
            print('Accessing file with wrong permissions.')
            raise IOError
        # Open access file
        with open(path+'/access.config', 'r') as f:
            reader = csv.reader(f)
            # Search for correct line
            for line in reader:
                # Assign args
                if bytes.fromhex(line[0]).decode('utf-8') == source:
                    args = line
                    break
                else:
                    args = None
    # Handle errors
    except IOError:
        raise IOError('Access file not found or has incorrect permissions.  Access denied.')
    if not args:
        raise ValueError('Data source name not found in access file.  Access denied.')

    user_name = bytes.fromhex(args[1]).decode('utf-8')
    password = bytes.fromhex(args[2]).decode('utf-8')
    return user_name, password
