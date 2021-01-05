#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/12/15 下午2:08

import os
import json
import pymssql
import datetime
import calendar
import pandas as pd
import datayes_csv_standard_export.myutils.logging_util as logging_util
from DBUtils.PooledDB import PooledDB

'''
此任务为导出全量csv文件
'''

pool_args = (0, 0, 0, 10, True, 0)
DB_72_datayesdb = """{"host":"10.21.139.72","user":"talend_load","password":"s9t5gNThn2vqWM7c","database":"datayesdb","charset":"utf8"}"""
DB_73_rpt = """{"host":"10.21.139.73","user":"uts_sync","password":"uts_sync","database":"research_rpt","charset":"utf8"}"""
DB_73_datayesdb = """{"host":"10.21.139.73","user":"uts_sync","password":"uts_sync","database":"datayesdb","charset":"utf8"}"""
DB_ct1_datayesdb = """{"host":"sh-dm-db04-ct1.datayes.com","user":"uts_sync","password":"uts_sync","database":"datayesdb","charset":"utf8"}"""

db_args_72_datayesdb = json.loads(DB_72_datayesdb, encoding='utf-8')
db_args_73_rpt = json.loads(DB_73_rpt, encoding='utf-8')
db_args_73_datayesdb = json.loads(DB_73_datayesdb, encoding='utf-8')
db_args_ct1_datayesdb = json.loads(DB_ct1_datayesdb, encoding='utf-8')

pool_72_datayesdb = PooledDB(pymssql, *pool_args, **db_args_72_datayesdb)
pool_73_rpt = PooledDB(pymssql, *pool_args, **db_args_73_rpt)
pool_73_datayesdb = PooledDB(pymssql, *pool_args, **db_args_73_datayesdb)
pool_ct1_datayesdb = PooledDB(pymssql, *pool_args, **db_args_ct1_datayesdb)

logger = logging_util.get_logger()


def export_main(dir_path):
    conn = pool_73_datayesdb.connection()
    sql = '''
        select 
            ADDRESS,
            END_DATE,
            HOLD_PCT,
            HOLD_VOL,
            ID,
            PARTY_NAME,
            SECURITY_ID,
            SHC_ID,
            SHC_NAME,
            TICKET_CODE,
            TRADE_CD,
            UPDATE_TIME,
            TMSTAMP
        from hk_shsz_detl with(nolock)
        where END_DATE>='20210104'
    '''
    df = pd.read_sql(sql, conn)
    file_name = 'hk_shsz_detl.csv'
    df.to_csv(os.path.join(dir_path, file_name), index=False, encoding='utf-8')
    logger.info('{} over --{}'.format(file_name, len(df)))


if __name__ == '__main__':
    export_main(r'/data/other/export_csv')
