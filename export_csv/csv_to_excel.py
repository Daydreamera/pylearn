#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2021/1/6 上午11:11

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


def transform(file_path, excel_dir):
    file_name = os.path.basename(file_path)
    try:
        df = pd.read_csv(file_path, encoding='utf-8', low_memory=False)
    except ValueError:
        logger.error(file_name + " Value Error")
    except FileNotFoundError:
        logger.error(file_name + " file not found!")
    if df is not None and len(df) >= 20:
        df = df.sample(20)
        df.to_excel(os.path.join(excel_dir, file_name.replace('csv', 'xlsx')), encoding='utf-8', index=False)
        logger.info(file_name + ' trans over')
    elif df is not None and len(df) > 0 and len(df) < 20:
        df.to_excel(os.path.join(excel_dir, file_name.replace('csv', 'xlsx')), encoding='utf-8', index=False)
        logger.info(file_name + ' trans over')
    else:
        logger.warning(file_name + ' is empty!')


def main(csv_dir, excel_dir):
    p = Pool(4)
    for file_path in glob.glob('{}/*.csv'.format(csv_dir)):
        p.apply_async(transform, args=(file_path, excel_dir))
    p.close()
    p.join()
    logger.info("all subprocesses done!")


if __name__ == '__main__':
    main(r'/51_datacopy/to_meifang/DKWL', r'/51_datacopy/to_meifang/DKWL/excel')
