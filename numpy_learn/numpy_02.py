#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/9 9:56 
# @Author : cong.wang
# @File : numpy_02.py 

import numpy as np

#  生成一维数组
arr_1 = np.arange(1, 10, 2, np.float)
print(arr_1)
print('*' * 100)
arr_2 = arr_1.astype(np.int)  # 显示转换数组类型
print(arr_2)
print('*' * 100)

# 数组切片-------------------------------------------------
arr = np.array([1, 2, 3, 4, 5])
print(arr)
arr[2:4] = 10
print(arr)

# -------------------------------------------------
arr_3 = np.zeros((5, 6), dtype=np.int)
num = 0
for i in range(arr_3.shape[0]):
    for j in range(arr_3.shape[1]):
        arr_3[i, j] = num
        num += 1

print(arr_3.T)  # 转置

# ---------------------------------------------------------
# 通用函数 分隔小数与整数
arr_4 = np.random.randn(7) * 5
print(arr_4)
zhengshu, xiaoshu = np.modf(arr_4)
print(zhengshu)
print(xiaoshu)
