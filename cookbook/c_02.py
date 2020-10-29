#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/22 16:21 
# @Author : cong.wang
# @File : c_02.py 

import heapq

nums = [1, 8, 2, 23, 7, -4, 18, 23, 42, 37, 2]
# nums = sorted(nums)
# print(nums[-3:])
print(heapq.nlargest(3, nums))

portfolio = [
    {'name': 'IBM', 'shares': 100, 'price': 91.1},
    {'name': 'AAPL', 'shares': 50, 'price': 543.22},
    {'name': 'FB', 'shares': 200, 'price': 21.09},
    {'name': 'HPQ', 'shares': 35, 'price': 31.75},
    {'name': 'YHOO', 'shares': 45, 'price': 16.35},
    {'name': 'ACME', 'shares': 75, 'price': 115.65}
]
cheap = heapq.nsmallest(1, portfolio, key=lambda s: s['price'])
expensive = heapq.nlargest(1, portfolio, key=lambda s: s['price'])
print(cheap, expensive)
