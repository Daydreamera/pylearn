#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/11/3 下午4:40

import pymssql
import pymysql
from datayes_csv_standard_export.myutils.db_util import read_dbconfig
from DBUtils.PooledDB import PooledDB

class DB_Base:
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
        self._create_connpool()

    # 创建连接池
    def _create_connpool(self):
        if self.port:
            self.connPool = PooledDB(pymysql, mincached=3, maxcached=10, maxconnections=20, blocking=True,
                                     host=self.host, user=self.user, password=self.password, database=self.database,
                                     port=self.port, charset='utf8')
        else:
            self.connPool = PooledDB(pymssql, mincached=3, maxcached=10, maxconnections=20, blocking=True,
                                     host=self.host, user=self.user, password=self.password, database=self.database,
                                     charset='utf8')

    # 获取连接
    def get_conn(self):
        try:
            self.conn = self.connPool.connection()
        except ConnectionRefusedError:
            print('数据库连接拒绝!')
        except ConnectionError:
            print('数据库连接异常！')
        return self.conn

    # 获取游标
    def get_cur(self):
        return self.get_conn().cursor()
