#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/9 17:34 
# @Author : cong.wang
# @File : create_sql.py 

import pymssql


# 獲取數據字典對外字段的查詢sql
def create_select_sql(tableName):
    sql = '''
    with temp1 as(
    select distinct name_en,filter_cond,date_constr,
    	(select short_name_en + ',' from(
    		select st.name_en,sc.short_name_en
    		from sys_table st 
    		inner join sys_column sc
    		on st.table_id = sc.table_id
    		where ((sc.is_pub = 1 and sc.SHORT_NAME_EN not in ('QA_ACTIVE_FLG','TMSTAMP')) or sc.SHORT_NAME_EN in ('TMSTAMP'))
    		--and st.name_en in
    		--('news_tagbd')
    		)t 
    	where t.NAME_EN=t1.name_en for xml path('')
    	) as short_name_en
    from (
    	select name_en,isnull(filter_cond,'1=1') filter_cond,date_constr
    	from sys_table
    	where name_en in(
    {}
    	))t1
    )
    select 'select ' + left(short_name_en,len(short_name_en)-1) + ' from ' + name_en +
    --select 'select ' + 'count(1)' + ' from ' + name_en +
    ' where ' + filter_cond + 
    case when
    date_constr is null then '' else + ' and ' +  date_constr + ' >= ''20170101'' and '+ date_constr + ' < ''20200803''\' end
    from temp1
    '''.format(tableName)

    conn = pymssql.connect('sh-dm-db04-r0.datayes.com', 'uts_sync', 'uts_sync', 'datayesdb')
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()

    select_sql = result[0][0].replace('TMSTAMP', 'cast(TMSTAMP as bigint) TMSTAMP').replace('Close', '[Close]')
    return select_sql


# 獲取按條件查詢條數的sql
def create_count_by_conditions_sql(table_name):
    sql = '''
            select 'select count(1) from ' + name_en + 
            case 
            when FILTER_COND is null and DATE_CONSTR is null then ''
            when FILTER_COND is not null and DATE_CONSTR is null then ' where ' + FILTER_COND
            when FILTER_COND is null and DATE_CONSTR is not null then ' where ' + DATE_CONSTR + '>=''20100801'' and ' + DATE_CONSTR + '<''20200802''\'
            when FILTER_COND is not null and DATE_CONSTR is not null then ' where ' + FILTER_COND + ' and ' + DATE_CONSTR + '>=''20100801'' and ' + DATE_CONSTR + '<''20200802''\' end
            from sys_table
            where name_en = '{}'
        '''.format(table_name)
    cur = pymssql.connect('sh-dm-db04-r0.datayes.com', 'uts_sync', 'uts_sync', 'datayesdb').cursor()
    cur.execute(sql)
    result_sql = cur.fetchall()[0]
    return result_sql[0]

#
# if __name__ == '__main__':
#     # print(create_select_sql('\'md_institution\''))
#
#     print(create_count_by_conditions_sql('sys_code'))
