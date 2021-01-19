#! usr/bin/python
# -*- coding: utf-8 -*-

import os
import glob
import logging
import pandas as pd
from multiprocess import Pool


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


logger = get_logger()

# ROW_FORMAT = '#@DatayesRow@#'
# COL_FORMAT = '#@DatayesCol@#'

ROW_FORMAT = '#@DyR@#'
COL_FORMAT = '#@DyC@#'


def count_special_file(file_path):
    if os.path.isfile(file_path) and os.path.basename(file_path)[-4:] == '.csv':
        basename = os.path.basename(file_path)
        with open(os.path.join(file_path), encoding='utf8') as f:
            string = f.read()
            count = string.count(ROW_FORMAT)
            logger.info(basename + ' : {}'.format(count - 1))
    elif os.path.isfile(file_path) and os.path.basename(file_path)[-4:] != '.csv':
        logger.info('this is not csv file')
    else:
        logger.error("incorrect file")


def count_normal_file(file_path):
    if os.path.isfile(file_path) and os.path.basename(file_path)[-4:] == '.csv':
        basename = os.path.basename(file_path)
        df = pd.read_csv(file_path, encoding='utf8')
        count = len(df)
        logger.info(basename + ' : {}'.format(count - 1))
    elif os.path.isfile(file_path) and os.path.basename(file_path)[-4:] != '.csv':
        logger.info('this is not csv file')
    else:
        logger.error("incorrect file")


def main(dir_path):
    p = Pool(4)
    for file_path in glob.glob('{}/*.csv'.format(dir_path)):
        p.apply_async(count_normal_file, args=(file_path,))
    p.close()
    p.join()
    logger.info("all subprocesses done!")



if __name__ == '__main__':
    dir_path = r'/51_datacopy/to_luanhua/CJZQ'
    main(dir_path)