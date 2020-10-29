#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/9/7 下午6:01

import os
import time
import json
import pymssql
import datetime
from DBUtils.PooledDB import PooledDB

pool_args = (0, 0, 0, 10, True, 0)
db_args = json.loads(
    """{"host":"sh-dm-db04-r0.datayes.com","user":"uts_sync","password":"uts_sync","database":"datayesdb","charset":"utf8"}""",
    encoding='utf-8')
conn_pool = PooledDB(pymssql, *pool_args, **db_args)


# begin_date_str = '20190901'
# end_date_str = '20200902'


def get_dateRange_list(begin_date_str, end_date_str):
    begin_date = datetime.datetime.strptime(begin_date_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d')
    # print(begin_date)

    date_diff = (end_date - begin_date).days
    date_list = []
    for i in range(date_diff):
        tmp = begin_date + datetime.timedelta(i)
        date_str = str(tmp.strftime('%Y%m%d'))
        # print(date_str)
        date_list.append(date_str)
    return date_list


def export_main(begin_date_str, end_date_str):
    conn = conn_pool.connection()
    cur = conn.cursor()
    date_list = get_dateRange_list(begin_date_str, end_date_str)
    for i in range(len(date_list)):
        sql = """
        select 
        UPDATE_TIME,
        TRADE_CD,
        cast(TMSTAMP as bigint) TMSTAMP,
        TICKET_CODE,
        SHC_NAME,
        SHC_ID,
        PARTY_NAME,
        ID,
        HOLD_VOL,
        HOLD_PCT,
        END_DATE,
        ADDRESS,
        SECURITY_ID 
        from hk_shsz_detl with(nolock) where END_DATE >= '{}' and END_DATE < '{}'
        """.format(date_list[i], date_list[i + 1])
        cur.execute(sql)
        file_name = r'/51_datacopy/to_jiaojiao/DCPM56319_20200916' + '//hk_shsz_detl' + date_list[i] + '.csv'  # 生成文件名
        # 文件若存在 则删除
        if os.path.exists(file_name):
            os.remove(file_name)
        # 新建目标文件
        f = open(file_name, 'a', encoding='utf8')
        # 写入列名
        columns = []
        for column in cur.description:
            columns.append(column[0])
        f.write('#@DatayesCol@#'.join(columns))
        f.write('#@DatayesRow@#')

        print(date_list[i] + ' strat to write:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        # 按行抓取数据
        data = cur.fetchone()
        count = 0
        while data is not None:
            data_to_str = []
            for da in data:
                data_to_str.append(str(da))
            f.write('#@DatayesCol@#'.join(data_to_str))
            f.write('#@DatayesRow@#')
            data = cur.fetchone()
            count += 1
        f.close()
        print('The number of dataRow is :', count)
        print(date_list[i] + ' is write over:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    cur.close()
    conn.close()


if __name__ == '__main__':
    export_main('20180101', '20190901')

    # print(get_dateRange_list('20190901', '20191004'))
