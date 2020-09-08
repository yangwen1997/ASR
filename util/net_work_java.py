#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: net_work_java
@time: 2020/7/9 0009 9:55
@desc: 
'''
import requests
import uuid
import random
import datetime
import hashlib
import time
import json


def get_random_str(num=12, current_time=str(int(round(time.time() * 1000)))):

    rs = random.sample(current_time, num)  # 生成一个 指定位数的随机字符串
    a = uuid.uuid1()  # 根据 时间戳生成 uuid , 保证全球唯一
    b = ''.join(rs + str(a).split("-"))  # 生成将随机字符串 与 uuid拼接
    return b  # 返回随机字符串


def create_sign(json_obj=dict(), sys_code=""):
    # 所有参与传参的参数按照accsii排序（升序）
    param_list = sorted(json_obj.items(), key=lambda d: d[0], reverse=False)

    md5_str = ""
    for item in param_list:
        # 空值不传递，不参与签名组串
        if item[0] is not None and len(item[0]) > 0:
            md5_str = md5_str + item[0] + "=" + item[1] + "&"

    md5_str = md5_str + "sysCode=" + sys_code
    md5_str = md5_str + "&secret=" + "a75c44fafd16bfc939dc0039d121760c"

    m = hashlib.md5(md5_str.encode())
    sign = m.hexdigest().upper()
    return sign


def post_gateway_request(param=dict()):

    sys_code = "dgg-intelligent-customer-plat"

    current_time = str(int(round(time.time() * 1000)))
    random_str = get_random_str(current_time=current_time)
    json_obj = {
        "time": current_time,
        "nonce": random_str,
        "data": json.dumps(param)
    }

    sign = create_sign(json_obj=json_obj, sys_code=sys_code)

    # 添加请求头
    headers = {
        'Content-Type': 'application/json',
        "sysCode": sys_code,
        "time": current_time,
        "nonce": random_str,
        "sign": sign
    }

    # 调用模型进行预测
    url = "https://tmicrouag.dgg188.cn/dgg-intelligent-customer-plat/semanteme/set_model_training.do"
    # loads()：将json数据转化成dict数据
    # dumps()：将dict数据转化成json数据
    # load()：读取json文件数据，转成dict数据
    # dump()：将dict数据转化成json数据后写入json文件
    s = json.dumps(param)
    print(param)
    r = requests.post(url, data=s, headers=headers)
    json_data = r.json()
    print(json_data)
    print()
    return json_data


def post_normal_request(param=dict()):
    # 添加请求头
    headers = {
        'X-User-Agent': '4b43c3f3-d817-4576-95b1-ad8519a2f14e'
    }
    # 调用模型进行预测
    url = "http://172.16.75.23:8093/semanteme/set_model_training.do"
    s = json.dumps(param)
    print("param:", param)
    r = requests.post(url, data=s, headers=headers)
    json_data = r.json()
    print(json_data)
    print()
    return json_data