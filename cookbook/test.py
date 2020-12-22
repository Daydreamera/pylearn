#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/23 9:42 
# @Author : cong.wang
# @File : test.py 

import os
import time
import json
import random
import pymssql
import datetime
import calendar
import pandas as pd
from DBUtils.PooledDB import PooledDB

# datayesdb_conn_pool = PooledDB(pymssql, **json.loads(
#     """{"host":"sh-dm-db04-r0.datayes.com", "user":"uts_sync", "password":"uts_sync", "database":"datayesdb"}""",
#     encoding='utf-8'))
# datayesdb_conn = datayesdb_conn_pool.connection()


# df = pd.read_csv('/data/other/ypred.csv', encoding='utf-8', header=None)
# df = df.rename(columns={0: 'col1', 1: 'col2'})
# print(df)
# df['col1'] = df['col1'].map(lambda x: 1 if x > 0.5 else 2)
# arr = tuple(df['col1'])
# print(arr)

# for file in os.listdir('/data/other/export_sql'):
#     os.system("mysql -uroot -p123456 cong.wang -e 'source {}'".format(os.path.join('/data/other/export_sql', file)))


os.system('cd /data/other/export_sql;rm *')