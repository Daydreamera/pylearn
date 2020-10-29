#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/12 下午1:24

import hdfs

client = hdfs.InsecureClient('http://bigdata01:50070', user='congwang')

print(client.list('/'))
print('=' * 100)
print(client.status('/user/congwang/input/aaa.txt'))

client.download('/user/congwang/input/aaa.                                                                                                             txt','/data/other/')