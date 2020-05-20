#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/19 10:22 
# @Author : cong.wang
# @File : DBConn.py

import MySQLdb
import pymssql
import pandas as pd
from DBUtils.PooledDB import PooledDB
import pylearn.export_csv.Utils as Utils


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
            self.connPool = PooledDB(MySQLdb, mincached=3, maxcached=10, maxconnections=20, blocking=True,
                                     host=self.host, user=self.user, passwd=self.password, db=self.database,
                                     port=self.port, charset='utf8')
        else:
            self.connPool = PooledDB(pymssql, mincached=3, maxcached=10, maxconnections=20, blocking=True,
                                     host=self.host, user=self.user, passwd=self.password, db=self.database,
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


# 读表
def readTable(tablename):
    sql = '''
    select * from {}
    '''.format(tablename)

    db = Database(Utils.readConfig('DBInfo.ini', 'Ubuntu', 'test'))
    my_cur = db.getConn().cursor()
    my_cur.execute(sql)
    result = my_cur.fetchall()
    return result


if __name__ == '__main__':
    # print(readTable('stu'))
    database = Database(Utils.readConfig('DBInfo.ini', 'Ubuntu', 'test'))
    studf = pd.read_sql('select * from stu', database.getConn())
    print(studf)
    '''
    行分隔符:#@DatayesRow@#
    列分隔符:#@DatayesCol@#
    '''
    studf.to_csv(r'C:/Users/Cong.Wang/Desktop/stu.csv', index=False, line_terminator='#@DatayesRow@#',
                 sep='#@DatayesCol@#')
