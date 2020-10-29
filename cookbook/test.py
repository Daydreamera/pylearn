#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/23 9:42 
# @Author : cong.wang
# @File : test.py 

import time
import json
import pymssql
import datetime
import calendar
import pandas as pd
from DBUtils.PooledDB import PooledDB

datayesdb_conn_pool = PooledDB(pymssql, **json.loads(
    """{"host":"sh-dm-db04-r0.datayes.com", "user":"uts_sync", "password":"uts_sync", "database":"datayesdb"}""",
    encoding='utf-8'))
datayesdb_conn = datayesdb_conn_pool.connection()


def myreadlines(f, newline):
    buf = ""
    while True:
        while newline in buf:
            pos = buf.index(newline)
            yield buf[:pos]
            buf = buf[pos + len(newline):]
        chunk = f.read(10)
        if not chunk:
            yield buf
            break
        buf += chunk


def read_csv(file_path):
    with open(file_path) as f:
        for line in myreadlines(f, "#"):
            print(line)


if __name__ == '__main__':
    file = r'C:\Users\Cong.Wang\Desktop\t.txt'
    read_csv(file)