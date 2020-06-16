#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/4 17:12 
# @Author : cong.wang
# @File : test20200604_1.py 

import os
from pylearn.export_csv.create_sql import create_select_sql

name = 'mkt_bond'

path = r'C:\Users\Cong.Wang\Desktop'

with open(r'D:/workspace/pyworkspace/pylearn/export_csv/table_name.txt', encoding='utf8') as f:
    tables = f.readlines()

print(tables)
for i in tables:
    i = i.replace('\n', '').replace('\r', '')
    tables_l = create_select_sql(i)
    print(tables_l[0].replace('TMSTAMP', 'cast(TMSTAMP as bigint) TMSTAMP'))
    file_name = path + '\\' + i[1:-1] + '.csv'
    print(file_name)
