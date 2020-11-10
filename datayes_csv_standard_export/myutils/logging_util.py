#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/11/9 上午10:10

import os
import logging


def get_logger():
    logger = logging.getLogger()
    logger.setLevel('INFO')
    BASIC_FORMAT = "%(asctime)s - %(levelname)s - %(funcName)s - %(message)s"
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
    # 控制台日志
    chlr = logging.StreamHandler()
    chlr.setFormatter(formatter)
    # 文件日志
    # fhlr = logging.FileHandler("../data/logs.txt")
    # fhlr.setFormatter(formatter)
    logger.addHandler(chlr)
    # logger.addHandler(fhlr)
    return logger


