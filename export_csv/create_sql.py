#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/9 17:34 
# @Author : cong.wang
# @File : create_sql.py 

import pymssql


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
    date_constr is null then '' else + ' and ' +  date_constr + ' >= ''20170701'' and '+ date_constr + ' < ''20200701''\' end
    from temp1
    '''.format(tableName)

    # sqls = []
    conn = pymssql.connect('sh-dm-db04-r0.datayes.com', 'uts_sync', 'uts_sync', 'datayesdb')
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    # for i in result:
    # for j in i:
    #     sqls.append(j)
    #     # print(j)
    select_sql = result[0][0].replace('TMSTAMP', 'cast(TMSTAMP as bigint) TMSTAMP').replace('Close', '[Close]')
    return select_sql
