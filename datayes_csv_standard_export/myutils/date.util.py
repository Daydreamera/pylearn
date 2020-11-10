#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/11/4 下午2:56

import time
import datetime


# 将时间戳转化成日期
def tms_to_date(TMSTAMP):
    return time.strftime('%Y-%m-%d', time.localtime(TMSTAMP))


def get_today():
    return datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')


if __name__ == '__main__':
    # print(tms_to_date(2132332323))
    print(get_today())
