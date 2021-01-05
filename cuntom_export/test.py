#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/12/15 下午2:51

"""
20201001-20210101
"""

import json
import pymssql
import pandas as pd
from DBUtils.PooledDB import PooledDB

pd.set_option('display.max_columns', None)


pool_args = (0, 0, 0, 10, True, 0)
DB_72_datayesdb = """{"host":"10.21.139.72","user":"talend_load","password":"s9t5gNThn2vqWM7c","database":"datayesdb","charset":"utf8"}"""
DB_73_rpt = """{"host":"10.21.139.73","user":"uts_sync","password":"uts_sync","database":"research_rpt","charset":"utf8"}"""
DB_73_datayesdb = """{"host":"10.21.139.73","user":"uts_sync","password":"uts_sync","database":"datayesdb","charset":"utf8"}"""
DB_ct1_datayesdb = """{"host":"sh-dm-db04-ct1.datayes.com","user":"uts_sync","password":"uts_sync","database":"datayesdb","charset":"utf8"}"""

db_args_73_datayesdb = json.loads(DB_73_datayesdb, encoding='utf-8')
pool_73_datayesdb = PooledDB(pymssql, *pool_args, **db_args_73_datayesdb)

def get_export_sql(table_name):
    pass