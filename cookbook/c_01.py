#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/22 15:25 
# @Author : cong.wang
# @File : c_01.py 


listA = [1, 2, 3, 4]
a, b, c, d = listA
print('a = %d\nb = %d\nc = %d\nd = %d' % (a, b, c, d))
print('-' * 100)

listB = ['China', 'America', 'Japan', 'Canada']
homeland, *other = listB
print(other)

