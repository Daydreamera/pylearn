#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/11/3 下午5:08

import os
import json
import pymssql
import configparser
import pandas as pd
from DBUtils.PooledDB import PooledDB

datayesdb_conn_pool = PooledDB(pymssql, **json.loads(
    """{"host":"sh-dm-db04-r0.datayes.com", "user":"uts_sync", "password":"uts_sync", "database":"datayesdb"}""",
    encoding='utf-8'))
conn = datayesdb_conn_pool.connection()


# 读取数据库配置
def read_dbconfig(path, option):
    info = {}
    cf = configparser.ConfigParser()
    if not os.path.exists(path):
        return 'file not exists'
    else:
        cf.read(path)
        for key in cf.options(option):
            if key == 'port':
                info[key] = int(cf.get(option, key))
            else:
                info[key] = cf.get(option, key)
        return info


# 获取表的数据字典目录
def get_table_catalog(table_name):
    sql = '''
    with temp1 as (
            SELECT
            t.TABLE_ID,
            t.NAME_EN,
            (SELECT CATG_NAME FROM datayesdb.dbo.sys_category WHERE CATG_CD = LEFT(r.CATG_CD,3)) AS 'L1',
            CASE
            WHEN LEN(r.CATG_CD) in (5,7) THEN (SELECT CATG_NAME FROM datayesdb.dbo.sys_category WHERE CATG_CD = LEFT(r.CATG_CD,5))
            WHEN LEN(r.CATG_CD) = 3 THEN NULL
            END AS 'L2',
            CASE
            WHEN LEN(r.CATG_CD) = 7 THEN (SELECT CATG_NAME FROM datayesdb.dbo.sys_category WHERE CATG_CD = r.CATG_CD)
            WHEN LEN(r.CATG_CD) in (3,5) THEN NULL
            END AS 'L3'
            FROM datayesdb.dbo.sys_catg_table_rel r 
            JOIN datayesdb.dbo.sys_category c on (r.CATG_CD = c.CATG_CD) 
            JOIN datayesdb.dbo.sys_table t on (r.TABLE_ID = t.TABLE_ID)
            )
            select TABLE_ID,NAME_EN,isnull('/'+L1,'')+isnull('/'+L2,'')+isnull('/'+L3,'') as PATH
            from temp1
            where NAME_EN in(
    '{table_name}'
            )
    '''.format(table_name=table_name)
    catalog_df = pd.read_sql(sql, conn)
    return catalog_df


# 获取导出sql
def get_export_sql(table_name, begin_date, end_date):
    sql = '''
        with temp1 as(
        select distinct name_en,filter_cond,date_constr,
        	(select short_name_en + ',' from(
        		select st.name_en,sc.short_name_en
        		from sys_table st 
        		inner join sys_column sc
        		on st.table_id = sc.table_id
        		where ((sc.is_pub = 1 and sc.SHORT_NAME_EN not in ('QA_ACTIVE_FLG')))
        		)t 
        	where t.NAME_EN=t1.name_en for xml path('')
        	) as short_name_en
        from (
        	select name_en,filter_cond,date_constr
        	from sys_table
        	where name_en = 
        '{table_name}'
        	)t1
        )
        select DATE_CONSTR,'select ' + left(short_name_en,len(short_name_en)-1) + ' from ' + name_en +
        case 
        when FILTER_COND is null and DATE_CONSTR is null then ''
        when FILTER_COND is not null and DATE_CONSTR is null then ' where ' + FILTER_COND
        when FILTER_COND is null and DATE_CONSTR is not null then ' where ' + DATE_CONSTR + '>=''{begin_date}0101'' and ' + DATE_CONSTR + '<''{end_date}0101''\'
        when FILTER_COND is not null and DATE_CONSTR is not null then ' where ' + FILTER_COND + ' and ' + DATE_CONSTR + '>=''{begin_date}0101'' and ' + DATE_CONSTR + '<''{end_date}0101''\' end
        as export_sql
        from temp1
        '''.format(table_name=table_name, begin_date=begin_date, end_date=end_date)
    sql_df = pd.read_sql(sql, conn)
    return sql_df


# 根据表名获取数据库连接
def get_conn(table_name):
    dydb_conn_pool = PooledDB(pymssql, **json.loads(
        """{"host":"sh-dm-db04-r0.datayes.com", "user":"uts_sync", "password":"uts_sync", "database":"datayesdb"}""",
        encoding='utf-8'))
    bigdata_conn_pool = PooledDB(pymssql, **json.loads(
        """{"host":"sh-dm-db04-r0.datayes.com", "user":"uts_sync", "password":"uts_sync", "database":"bigdata"}""",
        encoding='utf-8'))
    news_conn_pool = PooledDB(pymssql, **json.loads(
        """{"host":"sh-dm-db04-r0.datayes.com", "user":"uts_sync", "password":"uts_sync", "database":"news"}""",
        encoding='utf-8'))
    dydb_conn = dydb_conn_pool.connection()
    bigdata_conn = bigdata_conn_pool.connection()
    news_conn = news_conn_pool.connection()

    sql = '''
    select name from sys.tables where name = '{table_name}'
    union
    select name from sys.views where name = '{table_name}'
    '''.format(table_name=table_name)
    dydb_df = pd.read_sql(sql, dydb_conn)
    bigdata_df = pd.read_sql(sql, bigdata_conn)
    news_df = pd.read_sql(sql, news_conn)
    if len(dydb_df) == 1:
        conn = dydb_conn
    elif len(bigdata_df) == 1:
        conn = bigdata_conn
    elif len(news_df) == 1:
        conn = news_conn
    return conn


def get_db_count(table_name, condition_start, condition_end):
    select_sql = """
    select 'select count(1) from ' + name_en + ' with(nolock)'
            + case 
            when FILTER_COND is null and DATE_CONSTR is null then ''
            when FILTER_COND is not null and DATE_CONSTR is null then ' where ' + FILTER_COND
            when FILTER_COND is null and DATE_CONSTR is not null then ' where ' + DATE_CONSTR + ' between ''{start_time}''' + ' and ''{end_time}'''
            when FILTER_COND is not null and DATE_CONSTR is not null then ' where ' + FILTER_COND + ' and ' + DATE_CONSTR + ' between ''{start_time}''' + ' and ''{end_time}''' end
            + ';'
            from sys_table
            where name_en = '{table_name}'
    """.format(table_name=table_name, start_time=condition_start, end_time=condition_end)
    sql = pd.read_sql(select_sql, conn).iloc[0, 0]
    # print(sql)
    count = pd.read_sql(sql, get_conn(table_name))
    return count.iloc[0, 0]


def get_pub_columns(table_name):
    sql = '''
    select st.NAME_EN,sc.SHORT_NAME_EN from sys_table st
    join sys_column sc
    on st.table_id = sc.table_id
    where st.name_en = '{}'
    and sc.IS_PUB = 1
    '''.format(table_name)
    df = pd.read_sql(sql,conn)
    return list(df['SHORT_NAME_EN'])

if __name__ == '__main__':
    # print(str(get_table_catalog('pfund_nav_newest').iloc[0, 2]))mkt_limit
    # df = get_export_sql('sys_code', '20190101', '20200101')
    # df = get_db_count('news_company_score', '20190101', '20200101')
    df = get_pub_columns('md_institution')
    print(df)
