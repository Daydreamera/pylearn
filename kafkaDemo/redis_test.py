#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/12/2 下午2:24

import redis
import pymysql
import datetime
import pandas as pd
from DBUtils.PooledDB import PooledDB

mysql_pool = PooledDB(pymysql, host='192.168.229.130', database='datayes', user='root', password='123456', port=3306,
                      charset='utf8')
redis_pool = redis.ConnectionPool(host='localhost', port=6379)
mysql_conn = mysql_pool.connection()
# sr = redis.Redis(redis_pool)
sr = redis.Redis(host='localhost', port=6379)
# sr.set('one', '1', ex=100)
sql = 'select * from md_inst_cha'
df = pd.read_sql(sql, mysql_conn)
# for _, row in df.iterrows():
#     sr.set(row['ID'], str(list(row)),ex=3600)
for i in sr.keys():
    print(sr.get(i).decode('utf-8'))