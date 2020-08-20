#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/20 11:04 
# @Author : cong.wang
# @File : Utils.py 

import os.path
import datetime
import configparser

'''
获取昨天信息
'''
def getYestoday():
    today = datetime.date.today()
    oneday = datetime.timedelta(1)
    yestoday = today - oneday
    return yestoday


'''
获取当天日期
'''
def getToday():
    return datetime.date.today()


'''
获取两个日期之间的日期
(包含首尾日期)
'''


def dateRange(start_date_str, end_date_str, step=1):
    '''
    :param start_date_str: 开始日期字符串
    :param end_date_str: 结束日期字符串
    :param step: 间隔
    :return:
    '''
    start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d').date()
    end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d').date()
    days = (end_date - start_date).days + 1
    return [start_date + datetime.timedelta(i) for i in range(0, days, step)]


# 读取数据库信息
def readConfig(path, option, db):
    info = {'database': db}
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
