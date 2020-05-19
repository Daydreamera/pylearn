#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 10:18 
# @Author : cong.wang
# @File : SMTP.py

import smtplib
from email.mime.text import MIMEText


class SMTP():
    def __init__(self):
        # 服务器
        self.host = 'smtp.file_monitor.com'
        self.port = 25
        # 发送地址
        self.sourceAddr = '1764052725@qq.com'
        self.password = 'file_monitor@123'
        # 目标地址
        self.targetAddr = ['cong.wang@file_monitor.com']

    #   @classmethod
    def sendMail(self, content):
        msg = MIMEText(content, 'plain', 'utf-8')
        # msg['From'] = 'PZ@qq.com'
        msg['To'] = 'cong.wang@file_monitor.com'
        Subject = 'This is a test Email'
        msg['subject'] = Subject

        try:
            smtpObj = smtplib.SMTP(self.host, self.port)
            # smtpObj.ehlo()
            # smtpObj.starttls()
            # smtpObj.login(self.username, self.password)

            smtpObj.sendmail(self.sourceAddr, self.targetAddr, msg.as_string())
            print('发送成功')
        except smtplib.SMTPException:
            print('faild to send Email,Please checked the MailInformation!')
