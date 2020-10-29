# _*_ coding : utf-8 _*_

import pymssql
import pymysql
import configparser
import pandas as pd
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)

select_sql = '''select top 10 ID,
EVENT_ID,
NAME,
ABSTRACT,
SIZE,
KEYWORDS,
EVENT_KEYWORDS,
CLUSTER_STATUS,
EVENT_STATUS,
EVENT_TYPE,
EFFECTIVE_TIME,
UPDATE_TIME,
cast(TMSTAMP as bigint) TMSTAMP from ak_event
'''


def get_connect(config_path, section):
    cf = configparser.ConfigParser()
    cf.read(config_path)
    if len(cf.items(section)) == 4:
        return pymssql.connect(cf.get(section, 'host'),
                               cf.get(section, 'user'),
                               cf.get(section, 'password'),
                               cf.get(section, 'database'))
    else:
        return pymysql.connect(cf.get(section, 'host'),
                               cf.get(section, 'user'),
                               cf.get(section, 'password'),
                               cf.get(section, 'database'),
                               int(cf.get(section, 'port')))


def get_columns(config_path, section, sql):
    source_db = get_connect(config_path, section)
    my_cur = source_db.cursor()
    my_cur.execute(sql)
    columns = []
    for column in my_cur.description:
        columns.append(column[0])
    return columns


def get_data(config_path, section, sql):
    source_db = get_connect(config_path, section)
    my_cur = source_db.cursor()
    my_cur.execute(sql)
    columns = []
    for column in my_cur.description:
        columns.append(column[0])
    # print(columns)
    result = my_cur.fetchall()
    data = []
    for i in result:
        data.append(list(i))

    df = pd.DataFrame(data, columns=columns)
    # df.set_index(['ID'], inplace=True)
    return df
    # return result


def insert_data(config_path, section, sql):
    columns = ','.join(get_columns(r'DBInfo.config', 'source_db', select_sql)).replace('\'', '')
    re = get_data(r'DBInfo.config', 'source_db', sql)
    target_db = get_connect(config_path, section)
    my_cur = target_db.cursor()
    insert_sql = '''insert into ak_event({0}) values ({1})'''.format(columns, re[0])
    my_cur.execute(insert_sql)


# insert_data(r'DBInfo.config', 'target_db', select_sql)

df = get_data(r'DBInfo.config', 'source_db', select_sql)
print(df.columns.values)
