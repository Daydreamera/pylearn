#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/19 10:22 
# @Author : cong.wang
# @File : DBConn.py

import os
import time
import pymysql
import pymssql
import pandas as pd
from DBUtils.PooledDB import PooledDB
from pylearn.export_csv.create_sql import create_select_sql
import Utils


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
            print('数据库连接超时!')
        except ConnectionError:
            print('数据库连接异常！')
        return self.conn


# 将数据读入DataFrame并调用写文件方法
def read_table_as_dataframe(file_path):
    database = Database(Utils.readConfig('DBInfo.ini', 'db04_bigdata'))

    # 获取表名 生成sql
    with open(r'table_name.txt', encoding='utf8') as f:
        tableNames = f.readlines()

    for tableName in tableNames:
        tableName = tableName.replace('\n', '').replace('\r', '')  # 过滤换行符
        print(tableName)
        sql = create_select_sql(tableName)
        print(sql)
        # 读入pandas再进行解析 效率高但有内存局限
        df = pd.read_sql(sql[0].replace('TMSTAMP', 'cast(TMSTAMP as bigint) TMSTAMP'), database.getConn())
        file_name = file_path + '//' + tableName[1:-1] + '.csv'  # 生成文件名
        write_to_csv(file_name, df)


# 按行读取数据库数据并写入csv文件
def read_table_by_row(file_path, database):
    # 获取数据库连接
    database = Database(Utils.readConfig('DBInfo.ini', 'db04_' + database))
    conn = database.getConn()
    cur = conn.cursor()

    # 获取表名 生成sql
    with open(r'table_name.txt', encoding='utf8') as f:
        tableNames = f.readlines()
    for tableName in tableNames:
        tableName = tableName.replace('\n', '').replace('\r', '')  # 过滤换行符

        sql = create_select_sql(tableName)  # 调用方法生成查询sql语句
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
        f.write('#@DatayesCol@#'.join(columns))
        f.write('#@DatayesRow@#')

        print(tableName + ' strat to write:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        # 按行抓取数据
        data = cur.fetchone()
        count = 0
        while data is not None:
            data_to_str = []
            for i in data:
                data_to_str.append(str(i))
            f.write('#@DatayesCol@#'.join(data_to_str))
            f.write('#@DatayesRow@#')
            data = cur.fetchone()
            count += 1
        f.close()
        print('The number of dataRow is :', count)
        print(tableName + ' is write over:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        print()


def write_to_csv(file_path, df):
    '''
    行分隔符:#@DatayesRow@#
    列分隔符:#@DatayesCol@#
    '''
    if os.path.exists(file_path):
        os.remove(file_path)
    print('begin to write:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    with open(file_path, 'a', encoding='utf8') as f:
        for column in df.columns.values[:-1]:
            f.write(column + '#@DatayesCol@#')
        f.write(df.columns.values[-1] + '#@DatayesRow@#')
        for i in range(df.shape[0]):
            for data in df.iloc[i, :-1]:
                f.write(str(data) + '#@DatayesCol@#')
            f.write(str(df.iloc[i, -1]) + '#@DatayesRow@#')
    print('write over:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))


if __name__ == '__main__':
    # read_table_as_dataframe(r'C:\Users\Cong.Wang\Desktop')
    read_table_by_row(r'D:\TTT', 'datayesdb')