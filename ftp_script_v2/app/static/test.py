import random
import time
import logging
import pandas
import numpy
import os
from functools import reduce

import FTP
# создание экзмепляра FTP класса
ftp_manager = FTP.ManagerFTP()
connection = ftp_manager.get_connection
print(ftp_manager.cwd('/home/cesium/ftp'))
print(connection.set_pasv(True))
print(connection.pwd())
sss = connection.transfercmd('NLST')
data = sss.recv(204800)
print(sss.close)
print()
data = repr(data)
data = data.split('\n')
print(data)

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