#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/4 14:34 
# @Author : cong.wang
# @File : test20200604.py 

import time
import datetime

begindate = datetime.date(1996, 9, 1)
enddate = datetime.date.today()
subdays = (enddate - begindate).days + 1
print(enddate - begindate)
for i in range(subdays):
    date = begindate + datetime.timedelta(days=i)
    day = str(date)[-2:]
    weekday = '0' + str(time.strptime(str(date), '%Y-%m-%d').tm_wday + 1)
    # print(weekday)
    if day == weekday:
        print(date)
