# -*- coding: UTF-8 -*-

'''
多进程测试
'''

import pandas as pd
import pymssql
from time import sleep, ctime
from multiprocessing import Pool

tableList = ['sys_code',
             'fund_man_perf',
             'fund_type_avg',
             'fund_man_return',
             'fund_nav_gr_rk']


def write_csv(tableName, string):
    print(string)
    conn = pymssql.connect('sh-dm-db04-r0.datayes.com', 'uts_sync', 'uts_sync', 'datayesdb', charset='utf8')
    df = pd.read_sql('select top 100000 * from {}'.format(tableName), con=conn)
    print('{} write begin'.format(tableName))
    df.to_csv(r'D:\test\{}.csv'.format(tableName), sep=',')
    print('{} write over'.format(tableName))
    # conn.close()


if __name__ == '__main__':
    p = Pool(3)
    for table in tableList:
        p.apply_async(write_csv, args=(table, 'conn'))
        # print("111111111")
    print('waiting for all subprocesses done...')
    p.close()
    p.join()
    print('all subprocesses done!')
