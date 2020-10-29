#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/9/14 下午2:15

import time
import json
import pymysql
import datetime
from kafka import KafkaProducer
from DBUtils.PooledDB import PooledDB

pro = KafkaProducer(bootstrap_servers=['192.168.229.130:9092'])

pool_args = (0, 0, 0, 10, True, 0)
db_args = json.loads(
    """{"host":"192.168.229.130","user":"root","password":"123456","database":"cong.wang","port":3306,"charset":"utf8"}""",
    encoding='utf-8')
conn_pool = PooledDB(pymysql, *pool_args, **db_args)
conn = conn_pool.connection()
cur = conn.cursor()

cur.execute('select * from person')
data = cur.fetchall()
for row in data:
    # print(row)
    # for i in range(len(row)):
    #     msg = str(row[i])
    pro.send('tonglian', value=str(row).encode('utf-8'), key=str(row[0]).encode('utf-8'))
    time.sleep(5)

pro.close()
