#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: get_change_information_api.py
@time: 2020/8/26 9:33 上午
@desc: 监听mysql的日志，得到数据库增删改的数据
'''
import datetime
import json
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)
import pandas as pd
import numpy as np

"""
pymysqlreplication类似于java中的cancl要利用,要达到监听的效果，首先需要设置 同步账号权限

1. mysql中执行以下部分
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%' IDENTIFIED BY '123456';

flush privileges;
"""

class DateEncoder(json.JSONEncoder):
    """
    自定义类，解决报错：
    TypeError: Object of type 'datetime' is not JSON serializable
    """
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')

        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")

        elif isinstance(obj, bytes):
            return str(obj, encoding='utf-8');

        return json.JSONEncoder.default(self, obj)

class MonitorMysql(object):
    """监听mysql数据库的类"""
    def __init__(self,host = None,user = None,passwd = None,port = None):
        # 数据库配置信息
        self.mysql_setting = {
            'host': host,
            'port': port,
            'user': user,
            'passwd': passwd
        }

    def monitor(self, database_name = None,table_name = None):
        """
        进行数据库监听
        :param database_name: 需要监听的库名，为None则代表所有
        :param table_name: 需要监听的表名, 为None则代表所有
        :return:
        """
        # 1. 实例化 binlog 流对象
        stream = BinLogStreamReader(
            connection_settings = self.mysql_setting, # 数据库连接配置信息
            server_id = 101,  # slave标识，唯一
            blocking = True,  # 阻塞等待后续事件
            # 设定只监控写操作：增、删、改
            only_events=[
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent
            ],
            # resume_stream=True # 是否只监听最新更改
        )

        # 2. 获取 流数据
        for binlogevent in stream:
            # binlogevent.dump()  # 打印所有信息

            for row in binlogevent.rows:
                # 打印 库名 和 表名
                # event = {"schema": binlogevent.schema, "table": binlogevent.table}
                # event = {"schema": "ai_quilty", "table": "customer_label_match_202008"}
                if database_name is None:
                    datbase_name = binlogevent.schema

                if table_name is None:
                    table_name = binlogevent.table

                # event = {"schema": database_name, "table": table_name}
                # table_name = ["score_table","label_reminder_table"]

                event = {"schema": database_name, "table": table_name}

                # 获取流数据中，数据库操作类型 与 操作的数据
                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = row["values"]

                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"
                    event["data"] = row["after_values"]  # 注意这里不是values

                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = row["values"]

                # 3. 解析流数据 为 json
                # result = json.dumps(event, cls=DateEncoder, ensure_ascii=False, skipkeys=True)
                # print(result)
                # print(json.dumps(event, cls = DateEncoder,ensure_ascii=False))
                # print(type(event))
                # msg = json.dumps(event, cls=DateEncoder,ensure_ascii=False)
                table_name = event.get("table")  # 获取 表名


                # event = json.dumps(event1, cls=DateEncoder,ensure_ascii=False)
                # print()
                # sys.stdout.flush()

        # stream.close()  # 如果使用阻塞模式，这行多余了

def parsing_stream_data(self,stream_data):
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
    database_name = stream_data.get("schema")  # 获取 数据库名
    table_name = stream_data.get("table")  # 获取 表名
    action = stream_data.get("action")  # 获取 数据操作类型：insert、delete、update
    data = stream_data.get("data")  # 获取 具体数据

    # 将数据转为 dataframe 格式,方便与总的数据进行 新增 或 更新 或 删除
    if data is not None:
        data_df = pd.DataFrame(data, index=[0])
    else:
        data_df = pd.DataFrame()

    return database_name, table_name, action, data_df

def update_dataframe(action, stream_df,history_dataframe,unique_var = None,user_id = None,subset_list = None):
    """
    根据流数据进行dataframe的更改
    :param action: 数据库操作类型，insert、delete、update
    :param stream_df: 一个数据流的数据的dataframe
    :param history_dataframe: 总的dataframe
    :param subset_list: 进行pandas更改和删除的依据字段的列表,drop_duplicates
    :param user_id: 打分规则或标签字段的创建人id
    :return: 修改之后的dataframe
    """
    if user_id is not None:
        stream_df = stream_df[stream_df["user_id"] == user_id]

    if len(stream_df) == 0:
        return history_dataframe

    else:
        if action == "insert":
            history_dataframe = pd.concat([history_dataframe,stream_df], axis=0).reset_index()

        if action == "update":
            # 删除原来的那行
            unique_id = stream_df[unique_var].tolist()[0] # 找到 unique_id的值
            history_dataframe = history_dataframe[history_dataframe[unique_var] != unique_id]
            # 合并新行
            history_dataframe = pd.concat([history_dataframe,stream_df], axis=0).reset_index()

        if action == "delete":
            # 删除原来的那行
            unique_id = stream_df[unique_var].tolist()[0]  # 找到 unique_id的值
            history_dataframe = history_dataframe[history_dataframe[unique_var] != unique_id]
            # 合并新行
            history_dataframe.reset_index(inplace = True)

    return history_dataframe

if __name__ == '__main__':
    MonitorMysql(host = '192.168.254.149',user = 'root',passwd = None,port = 3306).monitor(database_name="intelligent_seat")
    # 'host': '192.168.254.149',
    # 'port': 3306,
    # 'user': 'root',
    # 'passwd': None
    """
    输出数据格式
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

