#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/23 9:42 
# @Author : cong.wang
# @File : test.py 

import os
import time
import json
import random
import pymssql
import datetime
import calendar
import pandas as pd
from DBUtils.PooledDB import PooledDB
from pip._vendor.distlib.compat import raw_input


# def f(a, b):
#     if b == 0:
#         print(a)
#     else:
#         f(b, a % b)
#
#
# a, b = input().split()
# f(int(a), int(b))

print(type(1/2))