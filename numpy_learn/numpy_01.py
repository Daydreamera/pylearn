#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/8 17:11 
# @Author : cong.wang
# @File : numpy_01.py 

import numpy as np

# 生成3x3随机整数数组
arr_1 = np.random.randint(1, 10, (3, 3))
print(arr_1)
print('*' * 100)
print(arr_1 * 10)  # 各位置x10
print('*' * 100)
print(arr_1 + arr_1)  # 各位置对应相加
print('*' * 100)
print(arr_1.shape, arr_1.dtype)  # 打印数组的维度与类型

print('=' * 100)
# 利用序列生成数组
listA = [[1, 2, 3, ], [3, 4, 5], [4, 5, 6]]
arr_2 = np.array(listA)
print(arr_2)
print('*' * 100)
print(np.zeros((3, 3), np.int))  # 创建全0数组
print('*' * 100)
print(np.zeros_like(arr_2))  # 创建一个形似其他数组的全0数组
print('*' * 100)
print(np.ones_like(arr_2))  # 创建一个形似其他数组的全1数组
