#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/7/7 下午4:24

import time
import pymysql
import pymssql
import logging
import datetime
import configparser
from data_check.create_sql import create_count_by_conditions_sql

file_name = 'dbinfo.config'
cf = configparser.ConfigParser()
cf.read(file_name)


# 列表比對
def list_compare(lsA, lsB):
    lsB_copy = lsB.copy()
    try:
        for i in lsA:
            lsB_copy.remove(i)
        if len(lsB_copy) == 0:
            return True
        else:
            return False
    except ValueError:
        return False


def get_connect(db):
    if len(cf.options(db)) == 4:
        return pymssql.connect(cf.get(db, 'host'),
                               cf.get(db, 'user'),
                               cf.get(db, 'password'),
                               cf.get(db, 'database'),
                               charset='utf8')
    else:
        return pymysql.connect(cf.get(db, 'host'),
                               cf.get(db, 'user'),
                               cf.get(db, 'password'),
                               cf.get(db, 'database'),
                               int(cf.get(db, 'port')),
                               charset='utf8')


def data_count_check(table_name, sdb_name, tdb_name):
    with open('check.log', 'a') as f:
        f.writelines(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()
                                   ) + '--' + table_name + '\n')
    source_cur = get_connect(sdb_name).cursor()
    target_cur = get_connect(tdb_name).cursor()

    count_sql = create_count_by_conditions_sql(table_name)

    source_cur.execute(count_sql)
    source_count = source_cur.fetchall()[0]

    target_cur.execute(count_sql)
    target_count = target_cur.fetchall()[0]

    if (source_count[0] >= target_count[0] & source_count[0] - target_count[0] <= 10):
        # print('The count of two table are same')
        with open('check.log', 'a') as f:
            f.write('source_count:' + str(source_count[0]) + '\n')
            f.write('target_count:' + str(target_count[0]) + '\n')
        return True
    else:
        with open('check.log', 'a') as f:
            # print('The count of two table are difference')
            f.write('source_count:' + str(source_count[0]) + '\n')
            f.write('target_count:' + str(target_count[0]) + '\n')
        return False

    source_cur.close()
    target_cur.close()


def column_check(table_name, sdb_name, tdb_name):
    # 首先检测数据量是否一致
    if data_count_check(table_name, sdb_name, tdb_name):
        # 从数据字典获取对外字段
        source_cur = get_connect('73_datayesdb').cursor()
        # 逻辑sql
        columns_from_datadict_sql = ''' 
        select sc.SHORT_NAME_EN from sys_table st
        join sys_column sc
        on st.table_id = sc.table_id
        where st.name_en = '{}'
        and (sc.IS_PUB = 1
        or sc.SHORT_NAME_EN = 'TMSTAMP')
        '''.format(table_name)
        # 获取数据字典的字段列表
        source_cur.execute(columns_from_datadict_sql)
        source_column_list = []
        for temp_col in source_cur.fetchall():
            source_column_list.append(temp_col[0])
        # print(source_column_list)

        # 从目标库获取字段
        target_cur = get_connect(tdb_name).cursor()
        # 逻辑sql
        if len(cf.options(tdb_name)) == 4:
            # sqlserver的sql
            columns_from_target_sql = '''
            select name from sys.columns 
            where object_id = object_id('{}')
            '''.format(table_name)
        else:
            # mysql的sql
            columns_from_target_sql = '''
            select c.column_name from information_schema.`TABLES` t
            join information_schema.`COLUMNS` c
            on t.table_name = c.table_name
            where t.table_name = '{}'
            and t.table_schema = '{}' 
            and c.table_schema = '{}'
            '''.format(table_name, cf.get(tdb_name, 'database'), cf.get(tdb_name, 'database'))
        # 获取目标库字段列表
        target_cur.execute(columns_from_target_sql)
        target_column_list = []
        for temp_col in target_cur.fetchall():
            target_column_list.append(temp_col[0])
        # print(target_column_list)

        source_cur.close()
        target_cur.close()

        # 字段比较
        if list_compare(source_column_list, target_column_list):
            with open('check.log', 'a') as f:
                f.write('the columns are same!\n')
            return source_column_list
        else:
            with open('check.log', 'a') as f:
                f.writelines(','.join(source_column_list))
                f.writelines(','.join(target_column_list))
            return False
    else:
        with open('check.log', 'a') as f:
            f.write('The count of two table are difference\n')
        return False


def get_column_type(db_type, table_name, column_name, conn):
    ms_get_column_type_sql = '''
    select sta.name,sco.name,sty.name from sys.tables sta 
    left join sys.columns sco on sta.object_id = sco.object_id
    left join sys.types sty on sco.system_type_id = sty.system_type_id
    where sta.name = '{}' 
    and sco.name = '{}'
    '''.format(table_name, column_name)

    my_get_column_type_sql = '''select table_name,column_name,data_type
    from information_schema.`COLUMNS`
    where table_name = '{}'
    and column_name = '{}'
    '''.format(table_name, column_name)

    cur = conn.cursor()
    if db_type == 'sqlserver':
        cur.execute(ms_get_column_type_sql)
    else:
        cur.execute(my_get_column_type_sql)
    data = cur.fetchone()
    column_type = data[2]
    return column_type


def data_compare(table_name, sdb_name, tdb_name):
    # 先进行数量`字段的判断
    columns = column_check(table_name, sdb_name, tdb_name)
    if columns:
        # 对sqlserver要特殊处理一些字段
        columns_ms = []
        for col in columns:
            type = get_column_type('sqlserver',table_name, col, get_connect('73_datayesdb'))
            if type == 'datetime' or type == 'date':
                col = 'convert(varchar(19),{},120)'.format(col)
                columns_ms.append(col)
            elif type == 'time':
                col = 'convert(varchar(19),{},108)'.format(col)
                columns_ms.append(col)
            else:
                columns_ms.append(col)

        # 对mysql可能也要特殊处理 先保留
        columns_my = []
        for col in columns:
            type = get_column_type('mysql',table_name, col, get_connect('his_export'))
            if type == 'time':
                col = 'date_format({},\'%H:%i:%s\')'.format(col)
                columns_my.append(col)
            else:
                columns_my.append(col)
    # 如果字段一致
    if columns:
        # 随机取出十条目标库的数据的主键
        pk_cur = get_connect('73_datayesdb').cursor()
        select_pk_sql = '''sp_pkeys \'{}\''''.format(table_name)
        pk_cur.execute(select_pk_sql)
        pk_info = pk_cur.fetchall()[0]
        pk = pk_info[3]
        # sqlserver的处理方式
        if len(cf.options(tdb_name)) == 4:
            ID_sql = '''select top 10 {} from {}'''.format(pk, table_name)
            target_cur = get_connect(tdb_name).cursor()
            target_cur.execute(ID_sql)
            IDList = []
            for tmp in target_cur.fetchall():
                IDList.append(tmp[0])
            print(IDList)
            for ID in IDList:
                DATA_sql = '''select {} from {} where {} = {}'''.format(','.join(columns), table_name, pk, ID)

                source_cur = get_connect(sdb_name).cursor()
                source_cur.execute(DATA_sql)
                source_data = source_cur.fetchall()
                source_data_list = []
                for data in source_data[0]:
                    source_data_list.append(data)

                target_cur.execute(DATA_sql)
                target_data = target_cur.fetchall()
                target_data_list = []
                for data in target_data:
                    target_data_list.append(data)

                if list_compare(source_data_list, target_data_list):
                    continue
                else:
                    print(source_data_list)
                    print(target_data_list)
                    return False
            print('數據質檢完成！')
            return True
        # mysql的处理方式
        else:
            ID_sql = '''select {} from {} limit 10'''.format(pk, table_name)
            target_cur = get_connect(tdb_name).cursor()
            target_cur.execute(ID_sql)
            IDList = []
            for tmp in target_cur.fetchall():
                IDList.append(tmp[0])
            print(IDList)
            target_cur.close()
            for ID in IDList:
                DATA_sql_my = '''select {} from {} where {} = {}'''.format(','.join(columns_my), table_name, pk, ID)
                DATA_sql_ms = '''select {} from {} where {} = {}'''.format(','.join(columns_ms), table_name, pk, ID)
                # print(DATA_sql_ms)
                source_cur = get_connect(sdb_name).cursor()
                source_cur.execute(DATA_sql_ms)
                source_data = source_cur.fetchall()
                source_data_list = []
                for data in source_data[0]:
                    source_data_list.append(str(data))

                target_cur = get_connect(tdb_name).cursor()
                target_cur.execute(DATA_sql_my)
                target_data = target_cur.fetchall()
                target_data_list = []
                for data in target_data[0]:
                    # 针对bit类型数据特殊处理
                    if data == b'\x01':
                        data = 'True'
                        target_data_list.append(str(data))
                    elif data == b'\x00':
                        data = 'False'
                        target_data_list.append(str(data))
                    else:
                        target_data_list.append(str(data))

                if list_compare(source_data_list, target_data_list):
                    continue
                else:
                    with open('check.log', 'a') as f:
                        f.write('source_data_list :{}\n'.format(source_data_list))
                        f.write('target_data_list :{}\n'.format(target_data_list))
                    return False
            with open('check.log', 'a') as f:
                f.write('the data are same!\n')
            print('數據質檢完成！')
            return True


with open(r'table_name.txt') as f:
    table_names = f.readlines()
    for table_name in table_names:
        table_name = table_name.replace('\n', '').replace('\r', '')
        print(table_name)
        data_compare(table_name, '73_datayesdb', 'his_export')
