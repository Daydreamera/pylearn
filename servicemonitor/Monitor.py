#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 13:49 
# @Author : cong.wang
# @File : Monitor.py 

import os
import wmi
import time
import logging
import configparser

# 获取配置信息
config = configparser.ConfigParser()
config.read('config.ini')
programPath = config.get('MonitorProgramPath', 'Path')
programName = config.get('MonitorProgramName', 'Name')

# 初始化
__wmi = wmi.WMI()
processList = []


def monitor():
    # 获取服务进程列表
    for processName in __wmi.Win32_Process():
        processList.append(processName.Name)

    # 判断目标进程是否在运行
    if programName in processList:
        print(programName + ' is running')
        # 清空进程列表 以便下轮监控
        del processList[:]
    else:  # 进程挂了 将状态写入日志
        f = open(r'./service_monitor.log', 'a')
        f.write('\n' + 'Server is not running,Begining to Restart Server...' + '\n')
        f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + '\n')
        # 重启服务
        os.startfile(programPath)
        f.write('Restart Server Successfully!')
        f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + '\n')
        # 关闭文件
        f.close()
        # 控制台打印
        print('Restart Server Successfully!')
        print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        # 清空进程列表 以便下轮监控
        del processList[:]


if __name__ == '__main__':
    while True:
        monitor()
        time.sleep(10)
