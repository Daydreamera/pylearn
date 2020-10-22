#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/9/18 16:40 
# @File : pandas_01.py

import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

file_path = r'C:\Users\Cong.Wang\Desktop\机场AMOS观测\20191215origin.csv'

df = pd.read_csv(file_path, nrows=5)
print(df)
print('-' * 100)
df_mean = df.groupby('CREATEDATE').agg(['mean'])
print(df_mean.columns)
df_mean.columns = ['_'.join(col).strip() for col in df_mean.columns.values]
df_mean = df_mean.reset_index()
# print(df_mean)
