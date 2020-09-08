#!/usr/bin/env python
# encoding: utf-8
"""
@File    : just_test.py
@Time    : 2020/8/7 11:01
@Author  : W
@Software: PyCharm
"""
import sys
import os
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.utils import MongoPool

uri = "mongodb://192.168.254.129:27017,192.168.254.130:27017,192.168.254.131:27017"

mongopool = MongoPool(uri)
conn = mongopool.get_conn()

test_set = mongopool.get_set(conn, "test_remark", "q_cdr_follow_test")
# test_db = conn.test_remark
# test_set = test_db.q_cdr_follow_test
for i in test_set.find():
    print(i)