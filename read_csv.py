#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/28 13:06
# @File : read_csv.py

import pandas as pd
import sys
import pymysql

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 100)

df = pd.read_csv(r'D:\zwtz\zwtz\hk_shsz_symbol.csv', error_bad_lines=False, encoding='utf8')
# nrows=100000)
print(df.__len__())

# conn = pymysql.connect('security03-dev.datayes.com', 'uts_sync', 'laV1Pa3sWE8jVoO2n', 'dydb_uts', 3309)
# my_cur = conn.cursor()
# result = my_cur.execute('select count(1) from vnews_body_v1')
# print(result)
