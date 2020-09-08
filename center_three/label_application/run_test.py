#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: run_test.py
@time: 2020/9/1 11:32 上午
@desc:
'''
import pandas as pd
import numpy as np
from center_three.util.utils import ReadFromMysql, InsertIntoMysql
from center_three.config.config import *
from center_three.label_application.operate_database.monitor_mysql import parsing_stream_data,DateEncoder,update_dataframe
import time
from interval import Interval

# 数据库监听
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (DeleteRowsEvent,UpdateRowsEvent,WriteRowsEvent)

# TODO 数据库查询对象
read_from_mysql = ReadFromMysql(host = mysql_host, user = mysql_user,password = mysql_password, port = mysql_port)

score_table_name = "score_table"
label_reminder_table_name = "label_reminder_table"
intelligent_seat_database_name = "intelligent_seat"

def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class MatchAndScore(object):
    def __init__(self,score_user_id = None,remind_user_id = None,):
        # 1. 读取标签打分项与打分规则
        self.score_item_list = [] # 打分项的列表

        if score_user_id is None:
            select_score_table_sql = "select * from {}.{}".format(mysql_common_database,score_table_name)
        else:
            self.score_user_id = score_user_id
            select_score_table_sql = "select * from {}.{} where user_id = {}".format(mysql_common_database,score_table_name,score_user_id)
        self.score_rule_df = read_from_mysql.select_from_table(database_name = mysql_common_database,SQL = select_score_table_sql) # 打分项的规则的dataframe，初始化时从数据库读取
        self.score_rule_df.columns = read_from_mysql.column_from_mysql(database_name = mysql_common_database,table_name = score_table_name)

        # 2. 标签提醒字段与文本
        self.remind_label_list = []
        if remind_user_id is None:
            select_label_reminder_sql = "select * from {}.{}".format(mysql_common_database,label_reminder_table_name)
        else:
            self.remind_user_id = remind_user_id
            select_label_reminder_sql = "select * from {}.{} where user_id = {}".format(mysql_common_database, label_reminder_table_name,remind_user_id)
        self.remind_label_df =  read_from_mysql.select_from_table(database_name = mysql_common_database,SQL = select_label_reminder_sql) # 打分项的规则的dataframe，初始化时从数据库读取
        self.remind_label_df.columns = read_from_mysql.column_from_mysql(database_name = mysql_common_database,table_name = label_reminder_table_name)
        print("==================================================初始化完毕===============================================")

    def update_dataframe(self,action, stream_df, history_dataframe, unique_var=None, user_id=None, subset_list=None):
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
                history_dataframe = pd.concat([history_dataframe, stream_df], axis=0).reset_index()

            if action == "update":
                # 删除原来的那行
                unique_id = stream_df[unique_var].tolist()[0]  # 找到 unique_id的值
                history_dataframe = history_dataframe[history_dataframe[unique_var] != unique_id]
                # 合并新行
                history_dataframe = pd.concat([history_dataframe, stream_df], axis=0).reset_index()

            if action == "delete":
                # 删除原来的那行
                unique_id = stream_df[unique_var].tolist()[0]  # 找到 unique_id的值
                history_dataframe = history_dataframe[history_dataframe[unique_var] != unique_id]
                # 合并新行
                history_dataframe.reset_index(inplace=True)

        return history_dataframe

    # TODO: 1. 更新数据库
    def monitor_mysql(self, database_name = intelligent_seat_database_name, table_name_list = None):
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
            server_id=110,  # slave标识，唯一
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
                # print(json.dumps(event, cls=DateEncoder, ensure_ascii=False))
                # event = json.dumps(event, cls=DateEncoder, ensure_ascii=False)

                database_name, table_name, action, data_df = parsing_stream_data(event)  # 解析成dataframe

                # 4. 进行所有数据的增删改
                if database_name != mysql_common_database:
                    continue
                else:
                    if table_name == score_table_name:
                        self.score_rule_df = self.update_dataframe(action=action, stream_df=data_df,
                                                              history_dataframe = self.score_rule_df,
                                                              subset_list = ["UserId", "Id"],
                                                              user_id = self.score_user_id)

                    elif table_name == label_reminder_table_name:
                        self.remind_label_df = self.update_dataframe(action=action, stream_df=data_df,
                                                                history_dataframe = self.remind_label_df,
                                                                subset_list = ["UserId", "Id"],
                                                                user_id = self.remind_user_id)

                    else:
                        continue


def label_match_reminder(label_list,rule_df,user_id = None,label_name_var = "LabelName",user_id_var = "UserId"):
    """
    进行label与待提醒的label的匹配
    :param label:
    :param rule_df: 从数据库导出的数据，或者经过等过更新的dataframe
    :param user_id:
    :return:
    """
    # 1. 进行label的匹配
    if user_id is not None:
        match_result_dataframe = rule_df.loc[rule_df[label_name_var].isin(label_list) & rule_df[user_id_var] == user_id]
    else:
        match_result_dataframe = rule_df.loc[rule_df[label_name_var].isin(label_list)]

    # 2. 获取匹配结果 list of dict
    columns = ["id",user_id,label_name_var,"RemindContent","IsValid","YtCode"]
    remind_list = match_result_dataframe.to_dict(orient="records")

    return remind_list

def scoring(score_item_list,rule_df,total_score = 100,user_id = None,is_continuous = 0,score_name_var = "ScoreName",user_id_var = "UserId"):
    """

    :param score_item_list: list of dict 待评分字典的list,[{"分贝":分贝值}]
    :param rule_df: 从数据库导出的数据，或者经过等过更新的dataframe
    :param user_id:
    :param score_name_var:
    :param user_id_var:
    :return:
    """
    # 0. 获取匹配字典
    score_item_dict = score_item_list[0]
    score_item_list = list(score_item_dict.keys())
    key = score_item_list[0]
    value = list(score_item_dict.values())[0]
    # print(type(value))

    # 0. 确认列名
    is_continuous_var = "IsContinuous"

    # 1. 进行score字段的匹配
    if user_id is not None:
        match_result_dataframe = rule_df.loc[rule_df[score_name_var].isin(score_item_list) & rule_df[user_id_var] == user_id]
    else:
        match_result_dataframe = rule_df.loc[rule_df[score_name_var].isin(score_item_list)]

    match_result_dict = match_result_dataframe.to_dict(orient = "records")
    # print(match_result_dict)

    # 2. 最终得分的dict
    score_dict = {}
    score_dict["score_item"] = key
    score_dict["value"] = value
    score_dict["score"] = None
    score_dict["score_weight"] = None

    # 3. 进行是否是连续型字段的判断
    if is_continuous == 1:
        # try:
        #     # 1. 整合区间
        #     for dic in match_result_dict:
        #         score = dic.get("score")
        #         weight = dic.get("score_weight")
        #         min_value = dic.get("min_value")
        #         max_value = dic.get("max_value")
        #
        #         # 构造数学区间
        #         if (min_value is not None) & (max_value is not None):
        #             inter_val = Interval(lower_bound = min_value,upper_bound = max_value,lower_closed = False,upper_closed = True)
        #
        #         elif (min_value is None) & (max_value is not None):
        #             inter_val = Interval(upper_bound = max_value,lower_closed = False,upper_closed = True)
        #
        #         elif (min_value is not None) & (max_value is None):
        #             inter_val = Interval(lower_bound = min_value,lower_closed = False,upper_closed = True)
        #
        #         else:
        #             continue
        #
        #         if value in inter_val:
        #             score_dict["score"] = score
        #             score_dict["score_weight"] = weight
        #             break
        #         else:
        #             continue
        # except Exception as e:
        #     print(e)

        # 整合区间
        for dic in match_result_dict:
            score = dic.get("score")
            weight = dic.get("score_weight")
            min_value = dic.get("min_value")
            max_value = dic.get("max_value")

            # 构造数学区间

            if (~np.isnan(min_value)) & (~np.isnan(max_value)):
                print(min_value)
                print(max_value)
                pass

            elif (np.isnan(min_value)) & (~np.isnan(max_value)):
                min_value = float("-inf")
                print(min_value)
                print(max_value)

            elif (~np.isnan(min_value)) & (np.isnan(max_value)):
                max_value = float("inf")
                print(min_value)
                print(max_value)

            else:
                continue

            if (value > min_value) & (value < max_value):
                score_dict["score"] = score
                score_dict["score_weight"] = weight
                break
            else:
                continue
    else:
        for dic in match_result_dict:
            print(dic)

            score = dic.get("Score")
            weight = dic.get("ScoreWeight")


            score_dict["score"] = score
            score_dict["score_weight"] = weight

    return score_dict


def before_insert_mysql(dataframe, fill_string_vars=["YtCode"]):
    """进行插入mysql之前的处理，防止null"""
    # 1. 填充空字符串的列
    for col in fill_string_vars:
        dataframe[col] = dataframe[col].fillna("")

    # 2. 转 np.NaN 为 None，防止插入数据库报null错误
    insert_df = dataframe.astype(object).where(pd.notnull(dataframe), None)
    insert_df.reset_index(drop=True, inplace=True)

    # 3. 得到插入数据库的数据
    return insert_df

def save_msyql(pid_list,database_name,table_name):
    """
    王辰的文本提取标签存数据库
    :param pid_list: 王辰的pid
    :param database_name:
    :param table_name:
    :return:
    """
    # 1. dict 转为 dataframe
    # test = {'id': 4, 'user_id': 0, 'score_pid': -1, 'score_name': '辱骂', 'score': -50, 'score_weight': 0.7, 'min_value': np.NaN,
    # 'max_value': np.NaN, 'is_continuous': 0, 'is_valid': 1, 'yt_code': np.NaN, 'created_at': np.NaN, 'updated_at': np.NaN,
    # 'deleted_at': np.NaN, 'extent_char1': np.NaN, 'extent_char2': np.NaN, 'extent_int1': np.NaN, 'extent_int2': np.NaN}
    temp_df = pd.DataFrame([pid_list])

    # 2. 插入数据库的对象
    # 测试库地址 正式库直接删除括号内代码默认正式库
    inset_into_mysql = InsertIntoMysql(host='192.168.254.149',
                                         user='root',
                                         password=None,
                                         port=3306)  # 产线

    # 3. 插入数据
    label_sort_insert = before_insert_mysql(temp_df)
    inset_into_mysql.insert_data_multi(label_sort_insert, database_name, table_name)


if __name__ == "__main__":
    test = pd.read_excel("/Users/sundali/Desktop/SunDaLi/Industrial_Projects/DGG/GIT/artificialintelligence/center_three/label_application/jupyter notebook/score测试.xlsx")


    # 2。进行匹配
    start = time.time()
    dic = scoring(score_item_list=[{"分贝": 70}], rule_df=test, is_continuous=1)
    print(dic)

    dic = scoring(score_item_list=[{"辱骂": "辱骂"}], rule_df=test, is_continuous=0)
    print(dic)
    print("耗时:%s"%(time.time()-start))


















