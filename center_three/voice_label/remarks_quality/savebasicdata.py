import os
import sys
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.utils import Read_From_Mysql,Insert_Into_Mysql,table_data
from center_three.voice_label.config import *
import pandas as pd
from datetime import datetime
import time
import json


class SaveBasic(object):

    def __init__(self):
        self.read = Read_From_Mysql()  # 创建存入mysql连接
        self.insert = Insert_Into_Mysql()  # 创建插入mysql连接

    def get_insert_data(self, pid, timestamp):
        """
        获取pid对应的busID与CustID
        :param pid: 通话ID
        :param timestamp: 时间戳用于当前年月表查询使用
        :return: 访问接口基础数据dataframe
        """
        table_name = call_createtime_table.format(table_data(timestamp))  # 表名
        sql = "select * from {} where uniqueid = '{}'".format(table_name, pid)  # sql 用以获得bus_info
        df = self.read.select_from_table(call_createtime_database, sql)
        columns_list = self.read.column_from_mysql(call_createtime_database, table_name)
        df.columns = columns_list
        if len(df) != 0:
            df = df[["uniqueid", "created_at", "bus_info"]]
            df = df[df.bus_info.notnull()]
            df = df.groupby("uniqueid").agg("last").reset_index()
            df.columns = ["pid", "call_time", "bus_info"]
            try:
                # TODO 目前字段名不确定
                # 从bus_info获得基本数据
                df["businessId"] = df.bus_info.apply(lambda x: json.loads(x)["businessId"])
                df["customerId"] = df.bus_info.apply(lambda x: json.loads(x)["customerId"])
                df["user_id"] = df.bus_info.apply(lambda x: json.loads(x)["userId"])
            except:
                return pd.DataFrame()
            df = df[["pid", "call_time", "businessId", "customerId", "user_id"]]
            df.call_time = df.call_time.astype("str")
            df["created_at"] = self.table_data(time.time())
            df.created_at = df.created_at.astype("str")
            df["status"] = 0
            return df
        else:
            return pd.DataFrame()

    def table_data(self, timestamp):
        """
        :param timestamp:  时间戳
        :return: 年月 (格式)->202007
        """
        dt = datetime.fromtimestamp(int(timestamp))
        date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
        return date

