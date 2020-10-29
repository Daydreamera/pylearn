#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/15 下午5:07

import os
import pymysql

with open(r'table_name.txt') as fd:
    table_name_list = fd.readlines()

print(table_name_list)

conn = pymysql.connect('10.20.202.56', 'han.yu', 'datayes@123', 'datayesdb', 3306, charset='utf8')
cur = conn.cursor()
if os.path.exists(r'create_table_script.txt'):
    os.remove(r'create_table_script.txt')
fw = open(r'create_table_script.txt', 'a')
for table_name in table_name_list:
    sql = '''show create table {}'''.format(table_name)
    cur.execute(sql)
    result = cur.fetchone()
    create_table_sc = result[1]
    fw.write(create_table_sc)
    fw.write(';\n')

fw.close()
