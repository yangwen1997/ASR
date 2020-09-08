#!/usr/bin/env python
# encoding: utf-8
"""
@File    : getlabel.py
@Time    : 2020/8/27 15:40
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
import re
sys.path.append(os.environ['PUSHPATH'])
from util.mapDGG import *
from center_three.util.utils import *
from center_three.config.config import *
from center_three.voice_label.voice_quality import test_func
warnings.filterwarnings("ignore")

start = "start"
end = "end"
pop_list = [PhoneDecibel,PhoneSpeechSpeed,PhoneDecibel,PhoneState,PhoneCallDuration]


def transform_datetime(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->202007
    """
    dt = datetime.datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
    return date


def transform_date(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->202007
    """
    dt = datetime.datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y-%m-%d').strip()
    return date


class GetLabel(object):
    """
        获取实时标签函数
    """
    def __init__(self):
        self.regex_values = None  # 正则的array(-1,)
        self.history = {}  # 通话历史数据 {pid:[]}
        # self.label = {}  # 通话的标签 {pid:[]},规则使用完存入数据库删除
        self.func = test_func.Test_Func()  # 动态调用数据库中函数
        self.redis_pool = GetRedisClusterConn(RedisHosts, RedisPassWord)  # 创建redis_pool
        self.read = ReadFromMysql(mysql_host, mysql_user, mysql_password, mysql_port)  # 获取读取mysql连接对象
        self.insert = InsertIntoMysql(mysql_host, mysql_user, mysql_password, mysql_port) # 获取插入mysql连接对象
        self.read.makepool(mysql_common_database)
        self.insert.makepool(mysql_common_database)

    def getregex(self):
        """
        :return: 返回正则表所有数据,保存在内存中
        """
        sql = "select * from {} where sid != 1".format(mysql_regex_table)  # 不取出sid==1 即备注正则
        regex_df = self.read.select_from_table(sql)
        regex_df.columns = self.read.column_from_mysql(mysql_regex_table)
        # 转为array_record
        regex_record = regex_df.to_records(index=False)
        regex_values = np.asarray(regex_record)
        self.regex_values = regex_values

    def get_typecode_regex(self, regex, type_code):
        """
        :param regex:  正则array数据
        :param type_code:  msg中的type_code
        :return: 业态与通用业态正则
        """
        basic_regex = regex[regex["yt_code"] == ""]  # 通用正则
        if type_code != "":
            type_regex = regex[regex["yt_code"] == type_code]
            regex = np.concatenate((basic_regex, type_regex))
            return regex
        else:
            return basic_regex

    def get_regex_status(self):
        """
        :return: 获取最新正则数据
        """
        redis_conn = self.redis_pool.connect()  # 获取redis连接
        msg = redis_conn.lrange(mysql_regex_table, 0, -1)  # 获取mysql_regex_table所有数据
        redis_conn.delete(mysql_regex_table)  # 清除获取mysql_regex_table的key
        if len(msg) != 0:
            data_list = []
            for i in msg:
                data_list.append(eval(i))
            df = pd.DataFrame(data_list)  # 转为DataFrame
            insert = df[df["event"] == "insert"]  # 获取insert相关id
            update = df[df["event"] == "update"]  # 获取update相关id
            delete = df[df["event"] == "delete"]  # 获取delete相关id
            if len(insert) != 0:  # 获取插入数据
                history_regex = pd.DataFrame(self.regex_values)  # 将内存中正则转为DataFrame
                ids = tuple(set(insert[insert["event"] == "insert"].id))  # 获取ids tuple
                sql = "select * from {} where id in {}".format(mysql_regex_table, ids)  # 正则SQL
                insert_regex = self.read.select_from_table(sql)  # 读取数据库获取
                regex = pd.concat([history_regex, insert_regex], axis=0).reset_index().drop_duplicates()  # 拼接去重
                # 转为array_record
                regex_record = regex.to_records(index=False)
                regex_values = np.asarray(regex_record)
                self.regex_values = regex_values
            if len(update) != 0:  # 获取更新数据
                history_regex = pd.DataFrame(self.regex_values)  # 将内存中正则转为DataFrame
                id_tuple = tuple(set(update[update["event"] == "update"].id))  # 获取ids tuple
                sql = "select * from {} where id in {}".format(mysql_regex_table, id_tuple)  # 正则SQL
                update_regex = self.read.select_from_table(sql)  # 读取数据库获取
                history_regex = history_regex[~history_regex.id.isin(id_tuple)]  # 不为更新ID的内存4正则
                regex = pd.concat([history_regex, update_regex], axis=0).reset_index().drop_duplicates()
                regex_record = regex.to_records(index=False)
                regex_values = np.asarray(regex_record)
                self.regex_values = regex_values
            if len(delete) != 0:  # 删除相关数据
                history_regex = pd.DataFrame(self.regex_values)
                id_list = list(set(delete[delete["event"] == "update"].id))
                history_regex = history_regex[~history_regex.id.isin(id_list)]
                regex_record = history_regex.to_records(index=False)
                regex_values = np.asarray(regex_record)
                self.regex_values = regex_values


    def match_label(self, msg):
        """
        :param msg: 传来的消息
        :return: 返回{pid:[{label1},{label2},{label3}]}
        """
        text = msg[PhoneText]  # 文本
        pid = msg[PhonePID]  # PID
        unique_id = msg[PhoneUniqueID]  # UniqueID
        type_code = msg[PhoneYTCode]  # 业态
        decibel = msg[PhoneDecibel]  # 分贝
        speech_speed = msg[PhoneSpeechSpeed]  # 语速
        talk_time = msg[PhoneCallDuration]  # 通话时长
        callState = msg[PhoneState]  # 通话状态
        callTime = msg[PhoneCreateTime]  # 通话时间
        regex_values = self.get_typecode_regex(self.regex_values, type_code)  # 根据type_code获取正则
        customer_regex = regex_values[regex_values["is_client"] == 1]  # 客户说
        dialogue_regex = regex_values[regex_values["is_client"] == 0]  # 对话
        called_label_list = []
        history_data = self.history.get(pid)
        if history_data == None:
            self.history[pid] = []
            self.history[pid].append({"text": text, "unique_id": unique_id})
        else:
            self.history[pid].append({"text": text, "unique_id": unique_id})
        history_data = self.history.get(pid)
        if pid != unique_id:
            sids = set(customer_regex["sid"])
            for sid in sids:
                customer_value_dict = {}
                regex_array = customer_regex[customer_regex["sid"] == sid]
                for customer_array in regex_array:
                    answer_regex = customer_array["question_word"]
                    answer_label = customer_array["question_result"]
                    answer_func = customer_array["question_func"]
                    labelid = customer_array["id"]
                    is_answer_match = re.search(answer_regex, text)
                    if is_answer_match != None:
                        customer_value_dict["pid"] = pid
                        customer_value_dict["unique_id"] = unique_id
                        customer_value_dict["LabelId"] = labelid
                        customer_value_dict["QuestionOriginalText"] = text
                        customer_value_dict["CallTime"] = callTime
                        customer_value_dict["CreateTime"] = str(transform_datetime(time.time()))
                        customer_value_dict[PhoneDecibel] = decibel
                        customer_value_dict[PhoneSpeechSpeed] = speech_speed
                        customer_value_dict[PhoneCallDuration] = talk_time
                        customer_value_dict[PhoneState] = callState
                        if answer_func != None:
                            answer_func_label, answer_func_result = getattr(self.func, answer_func)(text,
                                                                                               is_answer_match.group())
                            if answer_func_label != None:
                                customer_value_dict["QuestionLabel"] = answer_func_label
                                customer_value_dict["QuestionMatchText"] = answer_func_result
                                called_label_list.append(customer_value_dict)
                                break
                        else:
                            customer_value_dict["QuestionLabel"] = answer_label
                            customer_value_dict["QuestionMatchText"] = is_answer_match.group()
                            called_label_list.append(customer_value_dict)
                            break

        dialogue_label_list = []
        if pid != unique_id:
            count_num1 = 0
            business_text = []
            customer = ""
            for data_dict in reversed(history_data):
                history_unique_id = data_dict["unique_id"]
                history_text = data_dict["text"]
                if (count_num1 == 0) & (pid != history_unique_id):
                    customer += history_text
                    count_num1 += 1
                elif (count_num1 == 1) & (pid == history_unique_id):
                    business_text.append(history_text)
                else:
                    break
            business_text = ",".join(reversed(business_text))
            sids = set(dialogue_regex["sid"])
            for sid in sids:
                value_dict = {}
                regex_array = dialogue_regex[dialogue_regex["sid"] == sid]
                for regex in regex_array:
                    question_regex = regex["question_word"]
                    question_label = regex["question_result"]
                    question_func = regex["question_func"]
                    answer_regex = regex["answer_word"]
                    answer_label = regex["answer_result"]
                    answer_func = regex["answer_func"]
                    labelid = regex["id"]
                    is_question_match = re.search(question_regex, business_text)
                    if is_question_match != None:
                        is_answer_match = re.search(answer_regex, customer)
                        if is_answer_match != None:
                            value_dict["pid"] = pid
                            value_dict["unique_id"] = unique_id
                            value_dict["LabelId"] = labelid
                            value_dict["QuestionOriginalText"] = business_text
                            value_dict["AnswerOriginalText"] = customer
                            value_dict["CallTime"] = callTime
                            value_dict["CreateTime"] = str(transform_datetime(time.time()))
                            value_dict[PhoneDecibel] = decibel
                            value_dict[PhoneSpeechSpeed] = speech_speed
                            value_dict[PhoneCallDuration] = talk_time
                            value_dict[PhoneState] = callState
                            if question_func != None:
                                quetion_func_label, quetion_func_result = getattr(self.func, question_func)(business_text,
                                                                                                       is_question_match.group())
                                value_dict["QuestionLabel"] = quetion_func_label
                                value_dict["QuestionMatchText"] = quetion_func_result
                            elif question_func == None:
                                value_dict["QuestionLabel"] = question_label
                                value_dict["QuestionMatchText"] = is_question_match.group()
                            if answer_func != None:
                                answer_func_label, answer_func_result = getattr(self.func, answer_func)(customer,
                                                                                                   is_answer_match.group())
                                value_dict["AnswerLabel"] = answer_func_label
                                value_dict["AnswerMatchText"] = answer_func_result
                                dialogue_label_list.append(value_dict)
                            elif answer_func == None:
                                value_dict["AnswerLabel"] = answer_label
                                value_dict["AnswerMatchText"] = is_answer_match.group()
                                dialogue_label_list.append(value_dict)
                            break

        result_dict = {}
        if (callState == start) & (pid == unique_id):  # TODO 开始状态
            result_dict[PhoneState] = callState
            result_dict[PhonePID] = pid
            return result_dict
            # label = self.label.get(pid)
            # if label == None:
            #     self.label[pid] = []
            #     self.label[pid].append(result_dict)
            #     return result_dict
            # else:
            #     self.label[pid].append(result_dict)
            #     return result_dict
        if callState == end:
            self.history[pid] = []
        result = called_label_list + dialogue_label_list
        if len(result) != 0:
            result_dict[pid] = result
            return result_dict
            # label = self.label.get(pid)
            # if label == None:
            #     self.label[pid] = []
            #     for dic in result:
            #         self.label[pid].append(dic)
            #     result_dict[pid] = result
            #     return result_dict
            # else:
            #     for dic in result:
            #         self.label[pid].append(dic)
            #     result_dict[pid] = result
            #     return result_dict
        elif len(result) == 0:
            result_dict[PhoneState] = callState
            result_dict[PhonePID] = pid
            # label = self.label.get(pid)
            # if label == None:
            #     self.label[pid] = []
            #     self.label[pid].append(result_dict)
            # else:
            #     self.label[pid].append(result_dict)
            return result_dict

    def savedata(self, pid, result):
        conn = self.insert.getconn()
        if isinstance(result[pid], list) & (len(result[pid]) != 0):
            for dic in result[pid]:
                ID = dic.get("LabelId")
                if (ID != None) & (PhoneState in dic.keys()):
                    for i in pop_list:
                        try:
                            dic.pop(i)
                        except:
                            continue
                    for i in list(dic.keys()):
                        if dic[i] == None:
                            dic.pop(i)
                    sql1 = "INSERT INTO %s %s " % (mysql_label, tuple(dic.keys()))
                    sql1 = sql1.replace("'","")
                    sql2 = "VALUES {}".format(tuple(dic.values()))
                    sql = sql1+sql2
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                    cursor.close()
        conn.close()

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(GetLabel, cls).__new__(cls)
        return cls.instance




if __name__ == '__main__':
    label = GetLabel()
    label.getregex()

    param = dict()
    param[PhoneText] = "你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗"
    param[PhonePID] = "1"
    param[PhoneUniqueID] = "1"
    param[PhoneYTCode] = ""
    param[PhoneDecibel] = 65
    param[PhoneSpeechSpeed] = 25
    param[PhoneCallDuration] = 2222
    param[PhoneState] = "start"
    param[PhoneCreateTime] = "2020-02-02 02:02:02"
    param[PhoneSessionRole] = "dggsb"
    param[PhoneSceneID] = "123"

    hehe = dict()
    hehe[PhoneText] = "嗯,你打错了吧"
    hehe[PhonePID] = "1"
    hehe[PhoneUniqueID] = "0"
    hehe[PhoneYTCode] = ""
    hehe[PhoneDecibel] = 65
    hehe[PhoneSpeechSpeed] = 25
    hehe[PhoneCallDuration] = 2222
    hehe[PhoneState] = "0"
    hehe[PhoneCreateTime] = "2020-02-02 02:02:02"
    hehe[PhoneSessionRole] = "dggsb"
    hehe[PhoneSceneID] = "123"

    pei = dict()
    pei[PhoneText] = "就是价格问题上回有一个给报报下来了这100多有点贵啊"
    pei[PhonePID] = "1"
    pei[PhoneUniqueID] = "0"
    pei[PhoneYTCode] = ""
    pei[PhoneDecibel] = 65
    pei[PhoneSpeechSpeed] = 25
    pei[PhoneCallDuration] = 2222
    pei[PhoneState] = "0"
    pei[PhoneCreateTime] = "2020-02-02 02:02:02"
    pei[PhoneSessionRole] = "dggsb"
    pei[PhoneSceneID] = "123"

    cao = dict()
    cao[PhoneText] = "你妹的瓜锤"
    cao[PhonePID] = "1"
    cao[PhoneUniqueID] = "0"
    cao[PhoneYTCode] = ""
    cao[PhoneDecibel] = 65
    cao[PhoneSpeechSpeed] = 25
    cao[PhoneCallDuration] = 2222
    cao[PhoneState] = "0"
    cao[PhoneCreateTime] = "2020-02-02 02:02:02"
    cao[PhoneSessionRole] = "dggsb"
    cao[PhoneSceneID] = "123"

    start = time.time()
    a = label.match_label(param)
    b = label.match_label(hehe)
    print(b)
    print(time.time()-start)
    c = label.match_label(pei)

    d = label.match_label(cao)
    print(a)
    print(b)
    print(c)
    print(d)

    print(c)
    d = label.match_label(hehe)
    e = label.match_label(hehe)
    print(e)
    # d = label.match_label(cao)

    # print(label.label)

    #     for j in a[i]:
    #         print(j)

    # print(label.label)
    # my_threadpool(func_list)
    # print(len(label.regex_values))
    # print(label.match_label(param))
    # print(label.label)
    # print(label.history)







