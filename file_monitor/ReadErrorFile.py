#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 10:24 
# @Author : cong.wang
# @File : ReadErrorFile.py 

import os.path


# 获取错误文件
def readFile(file_path, error_file=[]):
    if not os.path.isdir(file_path):
        error_file.append(file_path)
    else:
        fileList = os.listdir(file_path)
        for tempFile in fileList:
            readFile(file_path + '\\' + tempFile, error_file)
    return error_file