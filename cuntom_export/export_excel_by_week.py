#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/12/15 下午2:33

import os
import json
import pymssql
import datetime
import calendar
import pandas as pd
from DBUtils.PooledDB import PooledDB
import datayes_csv_standard_export.myutils.logging_util as logging_util

'''
此任务为按周导出Excel文件
sql已限定好每周取一天数据 所以一个日期一个文件就可以
'''

pool_args = (0, 0, 0, 10, True, 0)
DB_72_datayesdb = """{"host":"10.21.139.72","user":"talend_load","password":"s9t5gNThn2vqWM7c","database":"datayesdb","charset":"utf8"}"""
DB_73_rpt = """{"host":"10.21.139.72","user":"talend_load","password":"s9t5gNThn2vqWM7c","database":"datayesdb","charset":"utf8"}"""
db_args_72_datayesdb = json.loads(DB_72_datayesdb, encoding='utf-8')
db_args_73_rpt = json.loads(DB_73_rpt, encoding='utf-8')
pool_72_datayesdb = PooledDB(pymssql, *pool_args, **db_args_72_datayesdb)
pool_73_rpt = PooledDB(pymssql, *pool_args, **db_args_73_rpt)

logger = logging_util.get_logger()


def date_to_str(date_fmt):
    return date_fmt.strftime('%Y%m%d')


# 通过sql获取df
def get_df_by_sql():
    conn = pool_72_datayesdb.connection()
    sql = '''
            select DISTINCT aa.WEEK_END_DATE as 日期,
            aa.TICKER_SYMBOL as 证券代码,
            aa.SEC_SHORT_NAME as 证券简称,
            case 
            when aa.指数300='1782' and aa.WEEK_END_DATE>=aa.INTO_DATE1 and aa.WEEK_END_DATE<aa.OUT_DATE1 then '是' 
            when aa.指数300='1782' and aa.WEEK_END_DATE>=aa.INTO_DATE1 and aa.OUT_DATE1 is null then '是' ELSE '否'
            end as 是否属于沪深300指数成份,
            case 
            when aa.指数300='1782' and aa.WEEK_END_DATE>=aa.INTO_DATE2 and aa.WEEK_END_DATE<aa.OUT_DATE2 then '是' 
            when aa.指数300='1782' and aa.WEEK_END_DATE>=aa.INTO_DATE2 and aa.OUT_DATE2 is null then '是' ELSE '否'
            end as 是否属于中证800指数成份,
            aa.LIST_DATE as 上市日期,
            aa.是否属于风险警示板,
            case when aa.CHG_STATUS='0' then '平盘' 
            when aa.CHG_STATUS='1' then '上涨(不含涨停)' 
            when aa.CHG_STATUS='-1' then '停牌(含暂停上市)' 
            when aa.CHG_STATUS='2' then '涨停(不含一字涨停)' 
            when aa.CHG_STATUS='3' then '一字涨停' 
            when aa.CHG_STATUS='4' then '下跌(不含跌停)' 
            when aa.CHG_STATUS='5' then '跌停(不含一字跌停)' 
            when aa.CHG_STATUS='6' then '一字跌停'
            end as 涨跌状态,
            aa.交易状态,
            aa.MARKET_VALUE as 总市值,
            aa.CLOSE_PRICE as 收盘价,
            aa.CHG_PCT as 涨跌幅
            from 
            (
            select b.WEEK_END_DATE,a.TICKER_SYMBOL,a.SEC_SHORT_NAME,a.LIST_DATE,
            case when b.WEEK_END_DATE>=a.LIST_DATE and b.WEEK_END_DATE<a.DELIST_DATE then '上市'
            when b.WEEK_END_DATE>=a.LIST_DATE and a.DELIST_DATE is null then '上市'
            when b.WEEK_END_DATE>=a.DELIST_DATE then '退市' 
            when b.WEEK_END_DATE<a.LIST_DATE then '未上市' 
            when a.LIST_DATE is null then '未上市'
            end as 交易状态,
            case when b.WEEK_END_DATE<c.EFF_DATE1 then '否'
            when b.WEEK_END_DATE>c.EFF_DATE2 then '否'
            when c.EFF_DATE1 is null and c.EFF_DATE2 is null then '否'
            when b.WEEK_END_DATE>=c.EFF_DATE1 and b.WEEK_END_DATE<c.EFF_DATE2 then '是'
            when b.WEEK_END_DATE>=c.EFF_DATE1 and c.EFF_DATE2 is null then '是'
            end as 是否属于风险警示板,
            d.SECURITY_ID 指数300,d.INTO_DATE INTO_DATE1,d.OUT_DATE OUT_DATE1,
            g.SECURITY_ID 指数800,g.INTO_DATE INTO_DATE2,g.OUT_DATE OUT_DATE2,
            e.CHG_STATUS,
            f.CLOSE_PRICE,
            f.CHG_PCT,
            f.MARKET_VALUE
            from
            (select SECURITY_ID,TICKER_SYMBOL,SEC_SHORT_NAME,LIST_DATE,LIST_STATUS_CD,DELIST_DATE
            from md_security where QA_ACTIVE_FLG='1' and DY_USE_FLG='1' 
            and ASSET_CLASS='e' and EXCHANGE_CD in('XSHG','XSHE') and TICKER_SYMBOL not like 'A%') a/*证券主表*/
            left join
            (select DISTINCT WEEK_END_DATE from md_trade_cal where QA_ACTIVE_FLG='1' and EXCHANGE_CD in('XSHG') 
            and CALENDAR_DATE >='20171210' and CALENDAR_DATE <='20201210') b/*交易日历*/
            on 1=1
            left join
            (SELECT DISTINCT t.SECURITY_ID,b.EFF_DATE EFF_DATE1,c.EFF_DATE EFF_DATE2
            from (SELECT *,row_number() over (partition by TICKER_SYMBOL order by EFF_DATE ) xh from equ_inst_sstate where (PARTY_STATE = '2' OR PARTY_STATE = '1' )
            and QA_ACTIVE_FLG=1) a 
            left join (SELECT *,row_number() over (partition by TICKER_SYMBOL order by EFF_DATE ) xh from equ_inst_sstate where (PARTY_STATE = '2' OR PARTY_STATE = '1' )
            and QA_ACTIVE_FLG=1) b 
            on a.TICKER_SYMBOL=b.TICKER_SYMBOL and b.PARTY_STATE = '2' 
            left join (SELECT *,row_number() over (partition by TICKER_SYMBOL order by EFF_DATE ) xh from equ_inst_sstate where (PARTY_STATE = '2' OR PARTY_STATE = '1' )
            and QA_ACTIVE_FLG=1) c on a.TICKER_SYMBOL=c.TICKER_SYMBOL and c.PARTY_STATE = '1' 
            LEFT JOIN (SELECT * from md_security where EXCHANGE_CD in ('xshg','xshe') and ASSET_CLASS ='e' and DY_USE_FLG=1
            and QA_ACTIVE_FLG=1) t 
            on a.TICKER_SYMBOL=t.TICKER_SYMBOL 
            where (c.xh-b.xh=1 or c.EFF_DATE is null)) c/*风险警示*/
            on a.SECURITY_ID=c.SECURITY_ID
            left join
            (select CONS_ID,IS_NEW,SECURITY_ID,INTO_DATE,OUT_DATE
            from idx_cons where QA_ACTIVE_FLG='1' and SECURITY_ID in('1782'/*沪深300*/)) d
            on a.SECURITY_ID=d.CONS_ID
            left join
            (select CONS_ID,IS_NEW,SECURITY_ID,INTO_DATE,OUT_DATE
            from idx_cons where QA_ACTIVE_FLG='1' and SECURITY_ID in('2107'/*中证800*/)) g
            on a.SECURITY_ID=g.CONS_ID
            LEFT JOIN 
            (select SECURITY_ID,TRADE_DATE,CHG_STATUS from mkt_equd_ind where QA_ACTIVE_FLG='1' and TRADE_DATE>='20171201' and TRADE_DATE<='20201212') e/*涨跌停状态*/
            on a.SECURITY_ID=e.SECURITY_ID
            and b.WEEK_END_DATE=e.TRADE_DATE
            LEFT JOIN 
            (select SECURITY_ID,TRADE_DATE,CLOSE_PRICE,CHG_PCT,MARKET_VALUE from mkt_equd 
            where QA_ACTIVE_FLG='1' and TRADE_DATE>='20171201' and TRADE_DATE<='20201212') f/*股票日行情*/
            on a.SECURITY_ID=f.SECURITY_ID
            and b.WEEK_END_DATE=f.TRADE_DATE
            ) aa
            where aa.WEEK_END_DATE is not null
            order by aa.WEEK_END_DATE,aa.TICKER_SYMBOL
    '''
    df = pd.read_sql(sql, conn)
    logger.info('DataFrame load over! --{}'.format(len(df)))
    return df


# 通过读取文件获取df
def get_df_by_file(file_path):
    # df = pd.read_excel(file_path)
    df = pd.read_csv(file_path, encoding='utf-8')
    logger.info('DataFrame load over! --{}'.format(len(df)))
    df['ticker'] = df['ticker'].map(lambda x: str(x).zfill(6))
    return df


def split_df_by_date(df, column):
    unique_date_mark = df.duplicated(column).map(lambda x: not x)
    unique_date_series = df[unique_date_mark][column]
    unique_date_list = list(unique_date_series)
    return unique_date_list


def export_main(dir_path, column):
    df = get_df_by_file(r'/51_datacopy/to_jiaoxuan/因子数据-周频---需拆分.csv')
    date_list = split_df_by_date(df, column)
    for date in date_list:
        df_tmp = df[df[column] == date]
        file_name = 'factor_{}.xlsx'.format(date.replace('-', ''))
        df_tmp.to_excel(os.path.join(dir_path, file_name), index=False, encoding='utf-8')
        logger.info('{} over --{}'.format(file_name, len(df_tmp)))


if __name__ == '__main__':
    export_main(r'/51_datacopy/to_jiaoxuan/因子数据-周频','tradeDate')
    # df = get_df_by_file(r'/51_datacopy/to_jiaoxuan/因子数据-周频---需拆分.csv')
    # print(df)