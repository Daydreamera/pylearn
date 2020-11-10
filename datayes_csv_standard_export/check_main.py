#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/11/9 上午10:51

import os
import pymssql
import pandas as pd
from pandas import DataFrame
from DBUtils.PooledDB import PooledDB
from datayes_csv_standard_export.dao.db_base import DB_Base
import datayes_csv_standard_export.myutils.db_util as db_util
import datayes_csv_standard_export.myutils.logging_util as logging_util

logger = logging_util.get_logger()


# 获取文件清单
def get_files(dir_path, file_dict):
    '''
    dir_path：文件的根目录
    '''
    for file in os.listdir(dir_path):
        if not os.path.isdir(os.path.join(dir_path, file)):
            if os.path.isfile(os.path.join(dir_path, file)) and file[-4:] == '.csv':
                file_dict[file] = os.path.join(dir_path, file)
            else:
                print('a valid file')
        else:
            get_files(os.path.join(dir_path, file), file_dict)
    return file_dict


def get_file_df(dir_path):
    file_dict = {}
    file_dict = get_files(dir_path, file_dict)
    file_df = DataFrame.from_dict(file_dict, orient='index', columns=['file_path'])
    file_df = file_df.reset_index().rename(columns={'index': 'file_name'})
    return file_df


# 获取文件数据量
def get_file_count(dir_path):
    df = get_file_df(dir_path)
    df['file_count'] = 0
    for index in range(len(df)):
        with open(str(df.loc[index, 'file_path'])) as f:
            df.loc[index, 'file_count'] = f.read().count('#@DyR@#') - 1
            logger.info('{} count is {}'.format(df.loc[index, 'file_name'], f.read().count('#@DyR@#') - 1))
    return df


# 获取数据库的数据量
def get_db_count(dir_path):
    file_df = get_file_df(dir_path)
    file_df['db_count'] = 0
    # file_df = file_df.reset_index().rename(columns={'index': 'id'})
    for i in range(len(file_df)):
        file_name = file_df.loc[i, 'file_name']
        if file_name[-8] == '2':
            table_name = file_name[:-9]
            begin_date = file_name[-8:-4] + '0101'
            end_date = str(int(begin_date[:4]) + 1) + '0101'
            file_df.loc[i, 'db_count'] = db_util.get_db_count(table_name, begin_date, end_date)
            logger.info('{} count is {}'.format(table_name, db_util.get_db_count(table_name, begin_date, end_date)))
        else:
            table_name = file_name[:-4]
            begin_date = 1975
            end_date = 2999
            file_df.loc[i, 'db_count'] = db_util.get_db_count(table_name, begin_date, end_date)
            logger.info('{} count is {}'.format(table_name, db_util.get_db_count(table_name, begin_date, end_date)))
    return file_df


# 获取文件字段
def get_file_columns(file_path):
    with open(file_path) as f:
        offset = 1024
        length = 1024
        col = f.read(length)
        while '#@DyR@#' not in col:
            f.seek(0, 0)
            length += offset
            col = f.read(length)
    file_columns_str = col.split('#@DyR@#')
    file_columns_list = file_columns_str[0].split('#@DyC@#')
    return file_columns_list


# 进行字段比对
def columns_compare(dir_path):
    file_df = get_file_df(dir_path)
    file_df['column_compare'] = False
    for index in range(len(file_df)):
        file_columns_list = get_file_columns(file_df.loc[index, 'file_path'])
        file_columns_str = ''.join(sorted(file_columns_list))
        file_name = file_df.loc[index, 'file_name']
        if file_name[-8] == '2':
            table_name = file_name[:-9]
            db_columns_list = db_util.get_pub_columns(table_name)
            db_columns_str = ''.join(sorted(db_columns_list))
        else:
            table_name = file_name[:-4]
            db_columns_list = db_util.get_pub_columns(table_name)
            db_columns_str = ''.join(sorted(db_columns_list))
        file_df.loc[index, 'column_compare'] = file_columns_str == db_columns_str
    return file_df


if __name__ == '__main__':
    db_df = get_db_count(r'/51_datacopy/Standard_DataPacket(CSV)/主表数据库')
    file_df = get_file_count(r'/51_datacopy/Standard_DataPacket(CSV)/主表数据库')
    columns_df = columns_compare(r'/51_datacopy/Standard_DataPacket(CSV)/主表数据库')
    df = db_df.merge(file_df, how='inner', on=['file_name', 'file_path'])
    df = df.merge(columns_df, how='inner', on=['file_name', 'file_path'])
    print(df)
