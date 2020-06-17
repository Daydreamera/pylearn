#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/4 17:12 
# @Author : cong.wang
# @File : test20200604_1.py 

import os
import time
import dateutil
import MySQLdb
import pymssql
import datetime
from pylearn.export_csv.create_sql import create_select_sql


def dateRange(start_date, end_date, step=1):
    days = (end_date - start_date).days + 1
    return [start_date + datetime.timedelta(i) for i in range(0, days, step)]


if __name__ == '__main__':
    date_list = dateRange(datetime.datetime.strptime('20200601', '%Y%m%d').date(),
                          datetime.datetime.strptime('20200616', '%Y%m%d').date())
    for i in date_list:
        print(i.strftime('%Y-%m-%d'))
