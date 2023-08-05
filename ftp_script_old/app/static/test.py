import random
import time
import logging
import pandas
import numpy
import os
from functools import reduce

time_iter_all_files = int(time.time())

time.sleep(10)
print(time_iter_all_files - int(time.time()))

"""

import ftplib
connection_config = {"password":"user",
                     "user_name":"cesium",
                     "host":"172.18.25.9",
                     "time_out":10,
                     "acct":"",
                     "log_level":10}
connection = ftplib.FTP(
    host=connection_config['host'],
    user=connection_config['user_name'],
    passwd=connection_config['password'],
    acct=connection_config['acct'],
    timeout=connection_config['time_out']
)

connection.cwd("/home/cesium/ftp")
print(connection.size('file_0.xml'))

"""