#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 10:33 
# @Author : cong.wang
# @File : run.py 

from file_monitor.ReadErrorFile import readFile
from file_monitor.SMTP import SMTP

if __name__ == '__main__':
    smtpObj = SMTP()
    smtpObj.sendMail('\n'.join(readFile(r'C:\Users\Cong.Wang\Desktop\dir')))
