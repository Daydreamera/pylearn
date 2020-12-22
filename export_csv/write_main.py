#! usr/bin/python
# -*- coding: utf-8 -*-
# @File : write_main.py

import os
import time
import pymysql
import pymssql
import pandas as pd
from DBUtils.PooledDB import PooledDB
from export_csv.create_sql import create_select_sql
import export_csv.myutils as myutils


class Database:
    def __init__(self, kwargs):
        if len(kwargs) == 5:
            self.host = kwargs['host']
            self.user = kwargs['user']
            self.password = kwargs['password']
            self.database = kwargs['database']
            self.port = kwargs['port']

        else:
            self.host = kwargs['host']
            self.user = kwargs['user']
            self.password = kwargs['password']
            self.database = kwargs['database']
            self.port = None
        self._createConnPool()

    # 创建连接池
    def _createConnPool(self):
        if self.port:
            self.connPool = PooledDB(pymysql, mincached=3, maxcached=10, maxconnections=20, blocking=True,
                                     host=self.host, user=self.user, password=self.password, database=self.database,
                                     port=self.port, charset='utf8')
        else:
            self.connPool = PooledDB(pymssql, mincached=3, maxcached=10, maxconnections=20, blocking=True,
                                     host=self.host, user=self.user, password=self.password, database=self.database,
                                     charset='utf8')

    # 获取连接
    def getConn(self):
        try:
            self.conn = self.connPool.connection()
        except ConnectionRefusedError:
            print('数据库连接拒绝!')
        except ConnectionError:
            print('数据库连接异常！')
        return self.conn


# 按行读取数据库数据并写入csv文件
def read_table_by_row(file_path, database, begin_date, end_date):
    """
    :param file_path:要写入的文件地址
    :param database:导出库
    :begin_date:开始时间
    :end_date:结束时间
    :return: None
    """
    # 获取数据库连接
    database = Database(myutils.readConfig('DBInfo.ini', 'db_' + database))
    conn = database.getConn()
    cur = conn.cursor()

    # 获取表名 生成sql
    with open(r'table_name.txt', encoding='utf8') as f:
        tableNames = f.readlines()
    for tableName in tableNames:
        tableName = tableName.replace('\n', '').replace('\r', '')  # 过滤换行符

        sql = create_select_sql(tableName, begin_date, end_date)  # 调用方法生成查询sql语句
        print(sql)
        cur.execute(sql)  # 执行SQL
        file_name = file_path + '//' + tableName[1:-1] + '.csv'  # 生成文件名
        # 文件若存在 则删除
        if os.path.exists(file_name):
            os.remove(file_name)
        # 新建目标文件
        f = open(file_name, 'a', encoding='utf8')
        # 写入列名
        columns = []
        for column in cur.description:
            columns.append(column[0])
        f.write('#@DyC@#'.join(columns))
        f.write('#@DyR@#')

        print(tableName + ' strat to write:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        # 按行抓取数据
        data = cur.fetchone()
        count = 0
        while data is not None:
            data_to_str = []
            for i in data:
                data_to_str.append(str(i))
            f.write('#@DyC@#'.join(data_to_str))
            f.write('#@DyR@#')
            data = cur.fetchone()
            count += 1
        f.close()
        print('The number of dataRow is :', count)
        print(tableName + ' is write over:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),end='\n')


if __name__ == '__main__':
    read_table_by_row(r'/51_datacopy/to_jiaojiao/ZKDSJ', 'linglei','20201001','20210701')
