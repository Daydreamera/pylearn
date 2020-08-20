#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/6/29 15:12 
# @Author : cong.wang
# @File : pacong.py

import requests
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}
res = requests.get('http://www.baidu.com/', headers=headers)
res.encoding = 'utf-8'
soup = BeautifulSoup(res.text, 'html.parser')
print(soup.select('#s-top-left > div > a'))
