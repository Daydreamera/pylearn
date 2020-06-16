#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 17:29 
# @Author : cong.wang
# @File : test.py

import pandas as pd
import os

# define file path
file_path = r'C:\Users\Cong.Wang\Desktop\ak_opinion.csv'


# read txt file to string
def read_file_as_str(file_path):
    if not os.path.isfile(file_path):
        raise TypeError(file_path + " does not exist")

    all_the_text = open(file_path, encoding='utf8').read()
    print(type(all_the_text))
    return all_the_text


# read the file
result_str = read_file_as_str(file_path)

# split the string by "#@DatayesRow@#"
result_str_list = result_str.split('#@DatayesRow@#')

# define the dataframe column name
columns_name_list = result_str_list[0].split('#@DatayesCol@#')

# begin transfor the results_list to dataframe
# 删除第一行
del result_str_list[0]
# for index, item in enumerate(result_str_list):
for index in range(len(result_str_list)):
    result_str_list[index] = result_str_list[index].split('#@DatayesCol@#')
df = pd.DataFrame(result_str_list, columns=columns_name_list)

# print the dataframe
print(df)
