#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/27 10:33 
# @Author : cong.wang
# @File : quicksort.py 

# 快速排序
def quicksort(preList):
    if len(preList) >= 2:
        flag = preList[0]
        left, right = [], []
        preList.remove(flag)
        for i in preList:
            if i >= flag:
                right.append(i)
            else:
                left.append(i)
        return quicksort(left) + [flag] + quicksort(right)
    else:
        return preList


testList = [1, 4, 2, 54, 65, 43, 243, 5454, 3, 43, 34, 43, 41]
print(quicksort(testList))
