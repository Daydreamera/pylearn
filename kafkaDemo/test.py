#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/9/14 下午2:51

import pymssql
import pandas as pd

conn = pymssql.connect(host='sh-dm-db04-r0.datayes.com', database='datayesdb', user='uts_sync', password='uts_sync',
                       charset='utf8')
sql = """
select * from hk_shsz_detl where id= 184378100
"""

df = pd.read_sql(sql, conn)
df.to_csv('./md.csv', encoding='utf-8',index=False)
print(df)

# df = pd.read_csv('/51_datacopy/to_jiaojiao/BHZQ/hk_shsz_detl_07.csv', encoding='utf-8',nrows=1000)
# print(df)
