#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: send_change_information_api.py
@time: 2020/8/26 9:34 上午
@desc:
'''
import requests
import json
url = "http://test"
data = {"key":"value"}
res = requests.post(url=url,data=json.dumps(data))

print(res.text)