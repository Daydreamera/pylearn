#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 17:29 
# @File : parse_csv.py

import os
import pandas as pd


# read txt file to string
def read_file_as_str(file_path):
    if not os.path.isfile(file_path):
        raise TypeError(file_path + " does not exist")
    all_the_text = open(file_path, encoding='utf8').read(10000)
    return all_the_text


def get_df(file_path):
    # read the file
    result_str = read_file_as_str(file_path)
    # split the string by "#@DatayesRow@#"
    result_str_list = result_str.split('#@DyR@#')
    # define the dataframe column name
    columns_name_list = result_str_list[0].split('#@DyC@#')
    # begin transfor the results_list to dataframe
    # 删除第一行
    del result_str_list[0]
    # for index, item in enumerate(result_str_list):
    for index in range(len(result_str_list)):
        result_str_list[index] = result_str_list[index].split('#@DyC@#')
    return pd.DataFrame(result_str_list, columns=columns_name_list)

if __name__ == '__main__':
    file_path = r'/51_datacopy/to_xiaoqian/md_institution.csv'
    df = get_df(file_path)
    print(df)
