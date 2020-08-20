#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/8/5 14:00 
# @Author : cong.wang
# @File : read_csv.py

import pandas as pd
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
path = r'Z:\标准化试用数据\基础数据\私募基金库\基金业绩\pfund_perf_indic\2017\pfund_perf_indic_2017_1.json'

df = pd.read_json(path, orient='split', encoding='utf8', )
# df = pd.read_csv(r'\\10.20.202.51\Datacopy\to_sudan\ZSZQ20200817\hk_shsz_hold_chg.csv',nrows=10000,skiprows=True)
# target_path = r'C:\Users\Cong.Wang\Desktop\test'
#
# if not os.path.exists(target_path):
#     os.mkdir(target_path)
#
# with open(file=file, mode='w') as f:
#     for i in range(df.__len__()):
#         f.write(str(df.iloc[i]))
#
# os.system('zip -r {}.zip {}'.format(file, file))
print(df.__len__())
