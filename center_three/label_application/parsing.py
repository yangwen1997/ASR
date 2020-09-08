#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: single_test.py
@time: 2020/8/27 10:06 上午
@desc:
'''
import pandas as pd

def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton

@Singleton
class GetRuledf(object):
    def __init__(self):
        test_dataframe = pd.DataFrame([[1, 2, 3], ["商务辱骂", "商务语速过快", "商务声音太大"]]).T
        test_dataframe.columns = ["label_id", "label_name"]
        test_dataframe.set_index("label_id", inplace=True)
        self.rule_df = test_dataframe


get_df = GetRuledf()

def parsing_stream_data(stream_data):
    """解析监听的数据库的流数据
    数据格式：
    {
        "schema": "demo",    # 数据库名
        "table": "student",  # 表名
        "action": "update",  # 动作 insert、delete、update
        "data": {            # 数据，里边包含所有字段
            "id": 26,
            "name": "haha",
            "age": 34,
            "update_time": "2019-06-06 16:59:06",
            "display": 0
        }
    }

    """
    database_name = stream_data.get("schema") # 获取 数据库名
    table_name = stream_data.get("table")     # 获取 表名
    action = stream_data.get("action")        # 获取 数据操作类型：insert、delete、update
    data = stream_data.get("data")            # 获取 具体数据

    # 将数据转为 dataframe 格式,方便与总的数据进行 新增 或 更新 或 删除
    if data is not None:
        data_df = pd.DataFrame(data,index = [0])
    else:
        data_df = pd.DataFrame()

    return database_name,table_name,action,data_df




