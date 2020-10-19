#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 10:18 
# @File : SMTP.py

import os
import smtplib
from email.mime.text import MIMEText


# 获取错误文件
def readFile(file_path, error_file=[]):
    if not os.path.isdir(file_path):
        error_file.append(file_path)
    else:
        fileList = os.listdir(file_path)
        for tempFile in fileList:
            readFile(file_path + '\\' + tempFile, error_file)
    return error_file


class SMTP():
    def __init__(self):
        # 服务器
        self.host = 'smtp.datayes.com'
        self.port = 25
        # 发送地址
        self.sourceAddr = 'cong.wang@ddd.com'
        self.password = ''
        # 目标地址
        self.targetAddr = ['cong.wang@datayes.com']

    #   @classmethod
    def sendMail(self, content):
        msg = MIMEText(content, 'plain', 'utf-8')
        # msg['From'] = 'PZ@qq.com'
        msg['To'] = 'cong.wang@datayes.com'
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


if __name__ == '__main__':
    smtpObj = SMTP()
    smtpObj.sendMail('\n'.join(readFile(r'C:\Users\Cong.Wang\Desktop\dir')))
