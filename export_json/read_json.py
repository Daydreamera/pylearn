#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/10/27 16:22 
# @File : read_json.py

import os
import pandas as pd

filepath_df = pd.read_excel(r'C:\Users\Cong.Wang\Desktop\json.xlsx')
for file_path in filepath_df['文件名称\路径']:
    file_name = os.path.basename(file_path)[:-5]
    # print(file_path)
    file_df = pd.read_json(file_path, orient='split', encoding='utf-8')
    file_df = file_df.sample(20)
    file_df.to_excel(r'D:\test\{}.xlsx'.format(file_name), index=False)

