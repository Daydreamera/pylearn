#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/21 上午11:07

import numpy as np
import pandas as pd

s_1 = pd.Series([1, 2, 3, 4], index=['a', 'b', 'c', 'd'])
# print(s_1)
# print('-' * 100)
# print(np.exp(s_1))  # e的次方
# print('-' * 100)
# print(s_1 * 10)

dict_1 = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5}
s_2 = pd.Series(dict_1)
# print(s_2)  # 按索引排序
# print('-' * 100)
# print(s_2.isnull())
# print('-' * 100)
# print(s_2.notnull())
# print('-' * 100)
# print(s_2.notna())

s_3 = pd.Series([1, 2, 3, 4])
print(s_3)
print('=' * 100)
s_3.index = ['one', 'two', 'three', 'four']
print(s_3)

s_4 = pd.Series([2, 3, 4, 5], index=['two', 'four', 'three', 'six'])
print(s_4)

print(s_3 + s_4)
