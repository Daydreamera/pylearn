#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/8/13 17:17 
# @Author : cong.wang
# @File : test20200813.py

import redis
import pymysql
import pandas as pd
import kafka

conn_mysql = pymysql.connect('192.168.229.130', 'root', '123456', 'cong.wang', 3306)
conn_redis = redis.Redis(host='192.168.229.130', port=6379)

# person_df = pd.read_sql('select * from person', conn_mysql)
# print(person_df.loc[1,'id'])
# for i in range(person_df.__len__()):
#     conn_redis.lpush('{}'.format(i), str(person_df.loc[i, 'id']), person_df.loc[i, 'name'], str(person_df.loc[i, 'age']),
#                      str(person_df.loc[i, 'TMSTAMP']))

#
# for key in range(len(person_df)):
#     print(conn_redis.lrange(key, 0, 10))

lit = ['a', 'v', 'v']
it = iter(lit)
print(next(it))
print(next(it))
