#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/22 上午10:29

import pymysql
import pandas as pd

conn = pymysql.connect('192.168.229.130', 'root', '123456', 'cong.wang', 3306, charset='utf8')

student_df = pd.read_sql('select * from student', conn)
score_df = pd.read_sql('select * from score', conn)
print(student_df)
print('=' * 100)
print(score_df)
print('=' * 100)
# print(student_df.set_index('sid').join(score_df.set_index('sid'), on='sid', how='inner'))  # 默認以索引作爲鏈接建 --不是很好用
# print('=' * 100)
# merge_df = pd.merge(student_df, score_df, how='left', sort=False)  # merge默認用兩個DateFrame共有的列作爲鏈接鍵
# print(merge_df)
# print('=' * 100)
# print(merge_df[pd.notna(merge_df['cid'])])

print(student_df.iloc[1, 2])  # 使用loc[index,column]來定位    iloc是數值化的用法
print(student_df.at[4, 'sid'])
