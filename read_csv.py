#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/8/5 14:00 
# @Author : cong.wang
# @File : read_csv.py

import pandas as pd
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
path = r'Z:\to_jiaojiao\WLCY20200918\mkt_idxd.csv'

# df = pd.read_json(path, orient='split', encoding='utf8', )
df = pd.read_csv(path, nrows=1000)
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
df.to_csv(r'Z:\to_jiaojiao\WLCY_demo\mkt_idxd.csv',index=False)
