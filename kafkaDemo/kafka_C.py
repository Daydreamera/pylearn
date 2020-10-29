#! usr/bin/python
# -*- coding: utf-8 -*-
# @Time : 2020/8/24 下午3:41


from kafka import KafkaConsumer

consumer = KafkaConsumer('tonglian', bootstrap_servers='192.168.229.130:9092', auto_offset_reset='earliest')
for msg in consumer:
    print(msg[6].decode('utf8'))