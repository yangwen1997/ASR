#!/usr/bin/env python
# encoding: utf-8
"""
@File    : listening_mysql.py
@Time    : 2020/8/28 17:07
@Author  : W
@Software: PyCharm
"""
import os
import sys
import numpy as np
import pandas as pd
import warnings
import datetime
import time
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
# sys.path.append(os.environ['PUSHPATH'])
from center_three.util.utils import *
from center_three.config.config import *
warnings.filterwarnings("ignore")

redis_pool = GetRedisClusterConn(RedisHosts, RedisPassWord)
redis_conn = redis_pool.connect()


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
            return str(obj, encoding='utf-8')
        else:
            return json.JSONEncoder.default(self, obj)


class ListeningMysql(object):

    def listening_mysql(self):
        stream = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=101,  # slave标识，唯一
            blocking=True,  # 阻塞等待后续事件
            # 设定只监控写操作：增、删、改
            only_events=[
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent
            ],
            # resume_s tream=True
        )
        for binlogevent in stream:
            # binlogevent.dump()  # 打印所有信息

            for row in binlogevent.rows:
                # 打印 库名 和 表名
                event = {"schema": binlogevent.schema, "table": binlogevent.table}
                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = row["values"]

                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"
                    event["data"] = row["after_values"]  # 注意这里不是values

                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = row["values"]
                # msg = json.dumps(event, cls=DateEncoder, ensure_ascii=False, skipkeys=True)
                # msg = json.loads(msg)
                msg = event
                database = msg["schema"]
                table_name = msg["table"]
                if (database == mysql_common_database) & ((table_name == mysql_regex_table) |
                                                          (table_name == mysql_score_table) |
                                                          (table_name == mysql_label_reminder_table)):
                    mysql_event = msg["action"]
                    data = msg["data"]
                    id_ = data["id"]
                    status = {"event": mysql_event, "id": id_}

                    print(status)
                    if table_name == mysql_regex_table:
                        redis_conn.lpush(mysql_regex_table, status)

                    if table_name == mysql_score_table:
                        print(status)
                        redis_conn.lpush(mysql_score_table, status)

                    if table_name == mysql_score_table:
                        print(status)
                        redis_conn.lpush(mysql_score_table, status)

                    if table_name == mysql_label_reminder_table:
                        print(status)
                        redis_conn.lpush(mysql_label_reminder_table, status)
                    # time.sleep(0.12)



def get_redis_data():
    # 删除redis中数据
    # redis_conn.delete(mysql_regex_table)
    # redis_conn.delete(mysql_score_table)
    # redis_conn.delete(mysql_label_reminder_table)

    # 读取redis数据
    # msg = redis_conn.lrange(mysql_regex_table, 0, -1)
    msg = redis_conn.lrange(mysql_score_table, 0, -1)
    # msg = redis_conn.lrange(mysql_label_reminder_table, 0, -1)
    
    print(msg)


if __name__ == '__main__':

    listening = ListeningMysql()
    # listening.listening_mysql()

    listening.listening_mysql()

    # get_redis_data()

