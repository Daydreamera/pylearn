#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/29 10:37 
# @Author : cong.wang
# @File : test20200729.py 

import os
import json
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


# person = '''{"name":"jack","age":24,"sex":"male"}'''
# p = json.loads(person)
# print(p)
# f = open('test.txt', 'w')
# json.dump(p, f, indent=4)

def print_time():
    print('The time of now is: ', datetime.datetime.now())


if __name__ == '__main__':
    bs = BlockingScheduler()
    bs.add_job(print_time, 'cron', hour=14, minute=24)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C    '))

    try:
        bs.start()
    except (KeyboardInterrupt, SystemExit):
        pass
