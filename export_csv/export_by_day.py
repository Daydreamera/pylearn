#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/9/7 下午6:01

import os
import time
import json
import pymssql
import datetime
import pandas as pd
from DBUtils.PooledDB import PooledDB

pool_args = (0, 0, 0, 10, True, 0)
db_args = json.loads(
    """{"host":"10.21.139.73","user":"uts_sync","password":"uts_sync","database":"datayesdb","charset":"utf8"}""",
    encoding='utf-8')
conn_pool = PooledDB(pymssql, *pool_args, **db_args)

dir_path = '/51_datacopy/to_jiaojiao/DCPM-59253'
# begin_date_str = '20190901'
# end_date_str = '20200902'


def str_to_date(date_str):
    date_fmt = datetime.datetime.strptime(date_str, '%Y%m%d')
    return date_fmt

def date_to_str(date_fmt):
    return date_fmt.strftime('%Y%m%d')

def export_main(begin_date_str, end_date_str):
    conn = conn_pool.connection()
    begin_date = str_to_date(begin_date_str)
    end_date = str_to_date(end_date_str)
    while begin_date < end_date:
        next_day = begin_date + datetime.timedelta(1)
        sql = """
            select 
            ID,
            SECURITY_ID,
            TICKER_SYMBOL,
            EXCHANGE_CD,
            TRADE_DATE,
            PER_CASH_DIV,
            PER_SHARE_DIV_RATIO,
            PER_SHARE_TRANS_RATIO,
            ALLOTMENT_RATIO,
            ALLOTMENT_PRICE,
            OPEN_PRICE,
            HIGHEST_PRICE,
            LOWEST_PRICE,
            CLOSE_PRICE,
            PRE_CLOSE_PRICE_2,
            OPEN_PRICE_2,
            HIGHEST_PRICE_2,
            LOWEST_PRICE_2,
            CLOSE_PRICE_2,
            TURNOVER_VOL,
            ACCUM_ADJ_FACTOR_2,
            UPDATE_TIME
            from mkt_equd_adj_af with(nolock)  
            where TRADE_DATE>='{}' 
            and TRADE_DATE<'{}'
            order by TICKER_SYMBOL
        """.format(begin_date, next_day)
        file_name = 'mkt_equd_adj_af-' + date_to_str(begin_date) + '.csv'  # 生成文件名
        file_path = os.path.join(dir_path,file_name)
        # 文件若存在 则删除
        if os.path.exists(file_path):
            os.remove(file_path)
        df = pd.read_sql(sql, conn)
        if df is not None and len(df) > 0:
            print(date_to_str(begin_date) + ' strat to write:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            df.to_csv(file_path, encoding='utf-8', index=False)
            print(date_to_str(begin_date) + ' is write over:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
            begin_date = next_day
        else:
            begin_date = next_day
            continue
    conn.close()


if __name__ == '__main__':

    export_main('20180101', '20180110')
