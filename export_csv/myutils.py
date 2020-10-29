#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/20 11:04 
# @Author : cong.wang
# @File : myutils.py

import os.path
import datetime
import configparser


# 获取昨天信息
def getYestoday():
    today = datetime.date.today()
    oneday = datetime.timedelta(1)
    yestoday = today - oneday
    return yestoday


# 获取当天日期
def getToday():
    return datetime.date.today()


# 读取数据库信息
def readConfig(path, option):
    info = {}
    cf = configparser.ConfigParser()
    if not os.path.exists(path):
        print('文件不存在')
    else:
        cf.read(path)
        for key in cf.options(option):
            if key == 'port':
                info[key] = int(cf.get(option, key))
            else:
                info[key] = cf.get(option, key)
        return info

