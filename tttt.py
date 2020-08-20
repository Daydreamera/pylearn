#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/23 13:59 
# @Author : cong.wang
# @File : tttt.py 

import pymssql
import pymysql
import pprint
import numpy as np
import pandas as pd
from DBUtils.PooledDB import PooledDB

np.set_printoptions(suppress=True)
pd.set_option('display.max_rows', None, 'display.max_columns', None)

pool_args = (0, 0, 0, 5, False, False, None)
db_kwargs = {'host': 'sh-dm-db04-r0.datayes.com', 'user': 'uts_sync', 'password': 'uts_sync',
             'database': 'datayesdb'}

pooldb = PooledDB(pymssql, *pool_args, **db_kwargs)

conn = pooldb.connection()
df = pd.read_sql(
    'select top 10 PARTY_ID,PARTY_FULL_NAME,REG_DATE,REG_PROVINCE,REG_CAP from md_institution where REG_DATE is not null',
    conn)
pprint.pprint(df)
print('=' * 100)
print(df[df['REG_DATE'].astype('str').str.replace('-', '').astype('int') > 20000101])
