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
此任务为按月导出Excel文件
'''

pool_args = (0, 0, 0, 10, True, 0)
DB_72_datayesdb = """{"host":"10.21.139.72","user":"talend_load","password":"s9t5gNThn2vqWM7c","database":"datayesdb","charset":"utf8"}"""
DB_73_rpt = """{"host":"10.21.139.72","user":"talend_load","password":"s9t5gNThn2vqWM7c","database":"datayesdb","charset":"utf8"}"""
db_args_72_datayesdb = json.loads(DB_72_datayesdb, encoding='utf-8')
db_args_73_rpt = json.loads(DB_73_rpt, encoding='utf-8')
pool_72_datayesdb = PooledDB(pymssql, *pool_args, **db_args_72_datayesdb)
pool_73_rpt = PooledDB(pymssql, *pool_args, **db_args_73_rpt)

logger = logging_util.get_logger()

BEGIN_DATE = '20200101'
END_DATE = '20210101'


# 字符串转日期
def str_to_date(date_str):
    return datetime.datetime.strptime(date_str, '%Y%m%d')


# 日期转化字符串
def date_to_str(date_fmt):
    return date_fmt.strftime('%Y%m%d')


# 获取当前日期的下个月起始
def get_next_month(date):
    return date + datetime.timedelta(calendar.monthrange(date.year, date.month)[1] - date.day + 1)


def export_main(dir_path):
    conn = pool_72_datayesdb.connection()
    begin_date = str_to_date(BEGIN_DATE)
    end_date = str_to_date(END_DATE)
    while begin_date <= end_date:
        sql = '''
            SELECT
                SEC_CODE 股票代码,
                SEC_NAME 股票简称,
                STAT_DATE 统计日期,
                STAT_TYPE 统计周期类型,
                STAT_PERIOD 统计周期,
                ORG_NUM 评级机构家数,
                BUY_NUM_ORG 买入评级机构数,
                ADD_NUM_ORG 增持评级机构数,
                HOLD_NUM_ORG 中性评级机构数,
                REDU_NUM_ORG 减持评级机构数,
                SELL_NUM_ORG 卖出评级机构数
            FROM
                rr_rating_stat a
            JOIN (
                SELECT DISTINCT
                    WEEK_END_DATE
                FROM
                    datayesdb..md_trade_cal
                WHERE
                    IS_OPEN = 1
                AND EXCHANGE_CD IN ('XSHG', 'XSHE')
            ) b ON a.STAT_DATE = b.WEEK_END_DATE
            WHERE
                STAT_DATE >= '{begin_date}'
            AND STAT_DATE < '{end_date}'
            AND ORG_NUM != 0
            ORDER BY
                STAT_DATE DESC,
                SEC_CODE,
                STAT_TYPE 
        '''.format(begin_date=begin_date, end_date=get_next_month(begin_date))
        df = pd.read_sql(sql, conn)
        file_name = 'file_{}_{}.xlsx'.format(begin_date.year, begin_date.month)
        df.to_excel(os.path.join(dir_path, file_name), index=False, encoding='utf-8')
        logger.info('{} over --{}'.format(file_name, len(df)))
        begin_date = get_next_month(begin_date)


if __name__ == '__main__':
    export_main(r'/51_datacopy/to_jiaoxuan/')
