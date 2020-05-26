#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 18:50 
# @Author : cong.wang
# @File : read_data_batch.py


import pymssql
import pandas as pd

conn = pymssql.connect(host='sh-dm-db04-r0.datayes.com', user='uts_sync', password='uts_sync', database='datayesdb',
                       charset='utf8')


# my_cur = conn.cursor()
# my_cur.execute('select * from cb_idx_weight')
# result = my_cur.fetch()
def read_data():
    for i in range(0, 100000000, 10000):
        sql = 'select * from cb_idx_weight where id >= {0} and id < {1}'.format(i, i + 10000)
        df = pd.read_sql(sql, conn)
        yield df


g = read_data()
while True:
    # 批次打印
    print(next(g))
    print('*' * 100)
