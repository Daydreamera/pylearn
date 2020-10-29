#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/23 10:04 
# @Author : cong.wang
# @File : c_03.py 

from collections import defaultdict

d = defaultdict(list)
d['a'].append(1)
d['a'].append(2)
d['a'].append(3)
d['b'].append(4)
d['b'].append(5)

# print(d)
print(d['a'])