#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/11/3 下午5:52

import os
import time
import datetime
import pandas as pd
import datayes_csv_standard_export.myutils.db_util as db_util
import datayes_csv_standard_export.dao.db_base as db_base

root_path = '/51_datacopy/Standard_DataPacket(CSV)'
begin_year = '2017'
end_year = '2020'
db = db_base.DB_Base(db_util.read_dbconfig('./source/DBInfo.ini', 'db_datayesdb'))


# 获取表清单
def get_table(path):
    tables = []
    with open(path) as f:
        for table in f.readlines():
            table = table.replace('\n', '').replace('\r', '').replace('\t', '').replace(' ', '')
            tables.append(table)
    return tables


# 获取文件名
def get_file_name(table_name, root_path, begin_year, filter_condition_flg):
    """
    Parameters
    ----------
    table_name： 要处理的表
    root_path： 标准数据包的根目录
    filter_condition_flg： 是否时间过滤
    begin_year： 时间条件（开始年份）
    end_year： 时间条件（结束年份）
    """
    catalog = db_util.get_table_catalog(table_name).iloc[0, 2]
    parent_path = root_path + catalog

    if not os.path.exists(parent_path):
        os.makedirs(parent_path)
        if filter_condition_flg:
            file_path = parent_path + '/' + table_name + '_' + begin_year + '.csv'
        else:
            file_path = parent_path + '/' + table_name + '.csv'

    else:
        if filter_condition_flg:
            file_path = parent_path + '/' + table_name + '_' + begin_year + '.csv'
        else:
            file_path = parent_path + '/' + table_name + '.csv'
    return file_path


def write_file(table_name, begin_date, end_date):
    cur = db.get_cur()
    while True:
        if begin_date < end_date:
            date_condition_column = \
            db_util.get_export_sql(table, begin_date=begin_year + '0101', end_date=end_year + '0101').iloc[
                0, 0]  # 时间过滤条件
            export_sql = db_util.get_export_sql(table_name, begin_date, str(int(begin_date) + 1)).iloc[0, 1]
            cur.execute(export_sql)
            file_name = get_file_name(table_name, root_path, begin_date, bool(date_condition_column))
            # 文件若存在 则删除
            if os.path.exists(file_name): os.remove(file_name)
            with open(file_name, 'a', encoding='utf8') as f:
                # 写入列名
                columns = []
                for column in cur.description:
                    columns.append(column[0])
                f.write('#@DyC@#'.join(columns))
                f.write('#@DyR@#')
                print(os.path.basename(file_name) + ' strat to write:',
                      time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
                # 批次抓取数据
                data_batch = cur.fetchmany(10000)
                while bool(data_batch):
                    data_batch_list = []
                    for data in data_batch:
                        data_list = []
                        for i in data:
                            data_list.append(str(i))
                        data_str = '#@DyC@#'.join(data_list)
                        data_batch_list.append(data_str)
                    data_batch_str = '#@DyR@#'.join(data_batch_list)
                    f.write(data_batch_str)
                    f.write('#@DyR@#')
                    data_batch = cur.fetchmany(10000)
                count = cur.rownumber
                print('The number of dataRow is :', count)
                print(os.path.basename(file_name) + ' is write over:',
                      time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), end='\n')
                if date_condition_column:
                    begin_date = str(int(begin_date) + 1)
                else:
                    break
        else:
            break


if __name__ == '__main__':
    tables = get_table('./source/tables')
    for table in tables:
        write_file(table, begin_year, end_year)
