#! usr/bin/python
# -*- coding: utf-8 -*-

import glob
import os
from sqlalchemy import exc
from sqlalchemy import create_engine
import pandas as pd


# read txt file to string
def csv_to_df(file_path):
    if not os.path.isfile(file_path):
        raise TypeError(file_path + " does not exist")
    all_the_text = open(file_path, encoding='utf8').read()  # 数值视内存决定
    # split the string by "#@DatayesRow@#"
    all_the_text_list = all_the_text.split('#@DatayesRow@#')
    # define the dataframe column name
    columns_name_list = all_the_text_list[0].split('#@DatayesCol@#')
    # begin transfor the results_list to dataframe
    # delete first line
    del all_the_text_list[0]
    # for index, item in enumerate(all_the_text_list):
    for index in range(len(all_the_text_list)):
        all_the_text_list[index] = all_the_text_list[index].split('#@DatayesCol@#')
    df = pd.DataFrame(all_the_text_list, columns=columns_name_list)
    return df


def main(csvdir, **kwargs):
    # 参数: csvdir, host, port, user, password, dbname
    engine = create_engine('mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}?charset=utf8'.format(**kwargs))
    #conn = engine.connect()
    for csv in glob.glob('%s/*.csv' % csvdir):
        tablename = os.path.splitext(os.path.basename(csv))[0]
        df = csv_to_df(csv)
        # 通联给的处理逻辑
        df.drop(columns=['TMSTAMP'], axis=1, inplace=True)
        if tablename in ["announcement", "event_desc", "mv_event_surprise"]:
            df.drop(columns=['UPDATE_TIME'], axis=1, inplace=True)
        elif tablename in ["event_window_return"]:
            df.drop(columns=['GROUP_TYPE'], axis=1, inplace=True)
        print("in process: %s" % tablename)
        print(df)
        for i in range(len(df)):
            try:
                df.iloc[i:i + 1].to_sql(tablename, engine, index=False, if_exists='append')
            except exc.IntegrityError:
                pass


if __name__ == "__main__":
    # print(csv_to_df('./csv/announcement_abstract.csv'))
    main(r"D:\test", host="192.168.229.130", port=3306, user="root", password="123456", dbname="tonglian")
