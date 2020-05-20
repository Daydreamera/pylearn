#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/5/14 18:50 
# @Author : cong.wang
# @File : Test_02.py 

def test(**args):
    print(args['b'])


extra = {'city': 'Beijing', 'job': 'Engineer'}


def person(kw):
    print(kw['city'])


person({'city': 'Beijing', 'job': 'Engineer'})
