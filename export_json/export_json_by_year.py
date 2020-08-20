# !/usr/bin/python
# encoding:utf-8
import pandas as pd
import pymssql
import pymysql
import time
import datetime
import logging  # 引入logging模块
import os.path
import time
import gc

# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Log等级总开关
# 第二步，创建一个handler，用于写入日志文件
rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
log_path = 'D:\标准化数据'
log_name = log_path + rq + '.log'
logfile = log_name
print(logfile)
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
# 第三步，定义handler的输出格式
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
# 第四步，将logger添加到handler里面
logger.addHandler(fh)

data_path = 'D:\标准化数据'

date_begin_str = '2017-01-01'

break_date_str = '2019-12-31 23:59:59'
break_date = datetime.datetime.strptime(break_date_str, '%Y-%m-%d %H:%M:%S')
table_list = [
    'pfund_idx',
    'pfund_mkt_idxd',
    'eco_info_pro',
    'announcement',
    'announcement_profitability',
    'announcement_abstract',
    'star_fdmt_cf_lt',
    'star_fdmt_is_new_q',
    'star_fdmt_is_indu_q',
    'star_fdmt_ind_lqd',
    'star_fdmt_ef_new'
]



con_pre = pymysql.connect(host='db-datayesdb-ro.wmcloud.com',
                          port=3313,
                          user='yang.bai',
                          password='ptttvaLIgMOAbD21n',
                          db='datayesdb',
                          charset='utf8',
                          cursorclass=pymysql.cursors.DictCursor)
cur_pre = con_pre.cursor()
cur_pre.execute("SET SESSION group_concat_max_len=102400")

con = pymssql.connect(host='sh-dm-db04-ct1.datayes.com', user='uts_sync', password='uts_sync', database='datayesdb')


def export_do(data_apth_var, table_name_var, date_begin_var, date_end_var, export_flg_1_var):
    sql1 = """
    select a.name_en as table_name,a.date_constr,
    case when a.date_constr is NULL THEN concat('select ',GROUP_CONCAT(b.short_name_en order by b.position),' from ',a.name_en, ' with (nolock) where ',case when filter_cond is null then '1=1' else filter_cond end )
    else concat('select ',GROUP_CONCAT(b.short_name_en order by b.position),' from ',a.name_en,' with (nolock) where ',a.date_constr," between '%s' and '%s'"," and ",case when filter_cond is null then '1=1' else filter_cond end )
    end as export_sql
    from sys_table a
    left join sys_column b
       on a.table_id=b.table_id
    where b.is_pub=1
    and a.name_en in (
    '%s'
    )
    group by a.name_en
    """ % (str(date_begin_var), str(date_end_var), table_name_var)
    sql2_pre = pd.read_sql(sql1, con_pre)
    for index, row in sql2_pre.iterrows():
        print(row['export_sql'])
        print(table_name_var)
        try:
            df = pd.read_sql(row['export_sql'].encode('utf-8'), con=con)
        except Exception as e:
            logger.info("%s  :  %s" % (table_name_var, e))
            return
        print("read to dataframe over")
        if export_flg_1_var == 'split':
            df.to_json(path_or_buf="%s_%s.json" % (data_apth_var + """/""" + table_name_var, str(date_begin_var.year)),
                       orient='split', force_ascii=False)
        if export_flg_1_var == 'all':
            df.to_json(path_or_buf="%s.json" % (data_apth_var + """/""" + table_name_var), orient='split',
                       force_ascii=False)
        print("export the file over")
    del df
    gc.collect()



def main(table_name_var):
    sql0 = """
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
    '%s'
            )
    """ % table_name_var
    sql0_pre = pd.read_sql(sql0, con)

    table_list = sql0_pre['NAME_EN'].values

    for temp_table in table_list:
        temp_path_var = sql0_pre[(sql0_pre['NAME_EN'] == temp_table)]['PATH'].values[0]
        file_path = '%s%s' % (data_path, temp_path_var)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        cur_date_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        cur_date = datetime.datetime.strptime(cur_date_str, '%Y-%m-%d %H:%M:%S')
        date_begin = datetime.datetime.strptime(date_begin_str, '%Y-%m-%d')
        date_begin_yearend = date_begin.replace(month=12, day=31, hour=23, minute=59, second=59)

        check_date_constr_sql = "select date_constr from sys_table where name_en='%s'" % temp_table
        check_date_constr_df = pd.read_sql(check_date_constr_sql, con_pre)

        if check_date_constr_df["date_constr"][0] is not None:
            if date_begin_yearend - date_begin > cur_date - date_begin:
                export_do(file_path, temp_table, date_begin, cur_date, 'split')
            else:
                temp_date_start = date_begin
                temp_date_end = date_begin_yearend
                while (1 == 1):
                    if temp_date_end > cur_date:
                        export_do(file_path, temp_table, temp_date_start, cur_date, 'split')
                        break
                    export_do(file_path, temp_table, temp_date_start, temp_date_end, 'split')
                    temp_date_start = temp_date_end + datetime.timedelta(seconds=1)
                    temp_date_end = temp_date_end + datetime.timedelta(days=365)
                    if temp_date_start > break_date:
                        break

        else:
            temp_date_start = date_begin
            export_do(file_path, temp_table, temp_date_start, cur_date, 'all')
            #     #
            #     # print(pd.read_json(path_or_buf="%s.json"%, orient='split'))


if __name__ == '__main__':
    for table_name_var in table_list:
        main(table_name_var)