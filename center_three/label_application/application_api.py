#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: run.py
@time: 2020/8/27 10:15 上午
@desc:
'''
import pandas as pd
import numpy as np
from center_three.util.utils import ReadFromMysql, InsertIntoMysql
from center_three.config.config import *
from center_three.label_application.operate_database.monitor_mysql import parsing_stream_data,DateEncoder,update_dataframe
import json

# 数据库监听
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)

# TODO 数据库查询对象
read_from_mysql = ReadFromMysql(host = mysql_host, user = mysql_user,password = mysql_password, port = mysql_port)

class LabelApplication(object):
    """进行标签打分的类"""
    def __init__(self,score_user_id = None,remind_user_id = None):
        # 1. 读取标签打分项与打分规则
        self.score_item_list = [] # 打分项的列表

        if score_user_id is None:
            select_score_table_sql = "select * from {}.{}".format(mysql_common_database,"score_table")
        else:
            self.score_user_id = score_user_id
            select_score_table_sql = "select * from {}.{} where user_id = {}".format(mysql_common_database,"score_table",score_user_id)
        self.score_rule_df = read_from_mysql.select_from_table(database_name= mysql_common_database,SQL = select_score_table_sql) # 打分项的规则的dataframe，初始化时从数据库读取
        self.score_rule_df.columns = read_from_mysql.column_from_mysql(database_name=mysql_common_database,table_name="score_table")

        # 2. 标签提醒字段与文本
        self.remind_label_list = []
        if remind_user_id is None:
            select_label_reminder_sql = "select * from {}.{}".format(mysql_common_database,"label_reminder_table")
        else:
            self.remind_user_id = remind_user_id
            select_label_reminder_sql = "select * from {}.{} where user_id = {}".format(mysql_common_database, "label_reminder_table",remind_user_id)
        self.remind_label_df =  read_from_mysql.select_from_table(database_name= mysql_common_database,SQL = select_label_reminder_sql) # 打分项的规则的dataframe，初始化时从数据库读取
        self.remind_label_df.columns = read_from_mysql.column_from_mysql(database_name=mysql_common_database,table_name="label_reminder_table")

        # 3. 读取王辰打分的属性

        # 4. 罗成打分的类属性

    # TODO: 1. 更新数据库
    def monitor_mysql(self, database_name = "intelligent_seat", table_name_list=None):
        """
        进行数据库监听
        :param database_name: 需要监听的库名，为None则代表所有
        :param table_name_list: 需要监听的表名的list, 为None则代表所有
        :return:
        """
        mysql_setting = {
            'host': mysql_host,
            'port': mysql_port,
            'user': mysql_user,
            'passwd': mysql_password}

        # 1. 实例化 binlog 流对象
        stream = BinLogStreamReader(
            connection_settings=mysql_setting,  # 数据库连接配置信息
            server_id= 110,  # slave标识，唯一
            blocking=True,  # 阻塞等待后续事件
            # 设定只监控写操作：增、删、改
            only_events=[
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent
            ],
            resume_stream=True  # 是否只监听最新更改
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
                print(json.dumps(event, cls=DateEncoder, ensure_ascii=False))
                event = json.dumps(event, cls=DateEncoder, ensure_ascii=False)

                database_name, table_name, action, data_df = parsing_stream_data(event) #解析成dataframe

                # 4. 进行所有数据的增删改
                if database_name != mysql_common_database:
                    continue
                else:
                    if table_name == "score_table":
                        self.score_rule_df = update_dataframe(action = action,stream_df = data_df,history_dataframe = self.score_rule_df,subset_list = ["user_id","score_id"],user_id = self.score_user_id)

                    elif table_name == "label_reminder_table":
                        self.remind_label_df = update_dataframe(action = action,stream_df = data_df,history_dataframe = self.remind_label_df,subset_list = ["user_id","label_id"],user_id = self.remind_user_id)

                    else:
                        continue


    # TODO: 2. 模拟王辰数据进行打分
    # {pid: [{"pid": "pid,"unique_id":"unique_id","LabelId","QuestionLabel","QuestionMatchText","QuestionOriginalText","
    #         AnswerLabel ".....}, {"pid":"pid,"unique_id": "unique_id", "LabelId", "QuetionLabel", "QuestionMatchText", "QuestionOriginalText", "AnswerLabel ".....}]}
    def get_asr_text_score(self,score_item_dict,score_rule_df):
        """
        从语音转文字的文本标签进行打分
        :param score_item_dict: 从王辰的文本提取标签的字典
        :param score_rule_df: 标签打分的规则
        :return:
        """

        # 1. 获取属性字典
        asr_text_dict = {"pid": "pid","unique_id":"unique_id","LabelId":"LabelId","QuetionLabel":"QuetionLabel",
                         "QuestionMatchText":"QuestionMatchText","QuestionOriginalText":"QuestionOriginalText","AnswerLabel":"AnswerLabel"}

        # 2. 获取文本标签
        question_label = asr_text_dict.get("QuestionLabel")
        answer_label = asr_text_dict.get("AnswerLabel")

        # 3. 进行标签匹配
        if question_label is not None:
            pass


        else:
            pass



    




    # TODO: 3. 模拟罗成进行打分

