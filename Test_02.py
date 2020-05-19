#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 18:50 
# @Author : cong.wang
# @File : Test_02.py 

path = r'C:\Users\Cong.Wang\Desktop\dmx_01.csv'  # 大约5G


def get_lines():
    # with open(path, 'r', encoding='utf-8').readlines() as f:
    f = open(path, 'r', encoding='utf-8')
    file = f.readlines()
    print('文件加载，一把读到内存中')
    return file


def get_lines_gene():
    # with open(path, 'r', encoding='utf-8').readlines() as f:
    f = open(path, 'r', encoding='utf-8')
    file = f.readlines()
    print('文件加载好了，现为生成器，读的过程加载内存')
    for i in file:
        yield i


if __name__ == '__main__':
    # for i in get_lines():
    #     print(i)  # MemoryError
    print(get_lines())
    # for i in get_lines_gene():
    #     print(i)  # ok
    print(get_lines_gene())
