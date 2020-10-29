#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/22 下午2:23

import numpy as np
import pandas as pd

# dict_1 = {'name': ['pzj', 'fjy', 'wzz'], 'age': [24, 26, 24], 'sex': ['female', 'female', 'male']}
# df_1 = pd.DataFrame(dict_1, columns=['name', 'sex', 'age'])
# print(df_1)
# print('=' * 100)
# # 重命名索引或列名
# df_2 = df_1.rename(index={0: 'one', 1: 'two', 2: 'three'})
# print(df_2)
# print('=' * 100)
# # 刪除可以使用drop
# print(df_2.drop('three'))
# print(df_2.drop('age', axis=1))


df_3 = pd.DataFrame(np.arange(16).reshape([4, 4]), index=['x1', 'x2', 'x3', 'x4'], columns=['y1', 'y2', 'y3', 'y4'])
df_4 = pd.DataFrame(np.arange(15).reshape([3, 5]), index=['x1', 'x2', 'x6'], columns=['y1', 'y2', 'y3', 'y4', 'y5'])

print(df_3)
print('=' * 100)
print(df_4)
print(len(df_4))
print('=' * 100)
print(df_3.add(df_4, fill_value=0))
