#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: get_reminder_context.py
@time: 2020/9/4 9:11 上午
@desc:
'''
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from center_three.util.utils import *
from center_three.config.config import *
import time

class GetReminderContent(object):
    """获取分数"""
    def __init__(self,user_id = None):

        # self.asr_dict = asr_dict # 王辰语音传过来的数据
        self.user_id = user_id
        self.read = ReadFromMysql(mysql_host, mysql_user, mysql_password, mysql_port)  # 获取读取mysql连接池
        self.insert = InsertIntoMysql(mysql_host, mysql_user, mysql_password, mysql_port)  # 获取读取mysql连接池
        self.history_df = self.read_original_df()
        self.redis_pool = GetRedisClusterConn(RedisHosts, RedisPassWord)  # 创建redis_pool

    def read_original_df(self):
        """初始化时读取所有的初始dataframe"""
        start0 = time.time()
        self.read.makepool(mysql_common_database)

        # 1. sql
        if self.user_id is None:
            sql = "select * from {}.{} where {} = 1".format(mysql_common_database,mysql_label_reminder_table,"IsValid") # IsValid = 1,代表可用
        else:
            sql = "select * from {}.{} where {} = 1 and {} = {}".format(mysql_common_database,mysql_label_reminder_table,"IsValid","UserId",self.user_id) # IsValid = 1,代表可用

        original_df = self.read.select_from_table(sql)
        original_df.columns = self.read.column_from_mysql(mysql_label_reminder_table)

        print("读取初始化df耗时：%s"%(time.time()-start0))
        return original_df

    def update_rule_df(self):
        """从redis读取更新数据的id,并进行总体数据的更新"""
        # 1. 读取redis数据
        start1 = time.time()
        redis_conn = self.redis_pool.connect()  # 获取redis连接
        msg_list = redis_conn.lrange(mysql_label_reminder_table, 0, -1)  # 获取 redis某个key的 所有数据, ["{"action":"insert","id":1}"]
        # redis_conn.delete(mysql_score_table)  # 清除获取redis中 mysql_score_table 的key

        # 2. 进行总体数据的更新
        msg_list = [eval(i) for i in msg_list]
        if len(msg_list) != 0:
            data_list = []
            for i in msg_list:
                data_list.append(eval(i))
            df = pd.DataFrame(data_list)  # 转为DataFrame
            insert = df[df["event"] == "insert"]  # 获取insert相关id
            update = df[df["event"] == "update"]  # 获取update相关id
            delete = df[df["event"] == "delete"]  # 获取delete相关id

            if len(insert) != 0:  # 获取插入数据
                history_df = self.history_df
                ids = tuple(set(insert[insert["event"] == "insert"].id))  # 获取ids tuple
                sql = "select * from {}.{} where id in {}".format(mysql_common_database, mysql_label_reminder_table,ids)  # SQL
                insert_df = self.read.select_from_table(sql)  # 读取数据库获取
                changed_df = pd.concat([history_df, insert_df], axis=0).reset_index().drop_duplicates()  # 拼接去重

                self.history_df = changed_df

            if len(update) != 0:  # 获取更新数据
                history_df = self.history_df
                id_tuple = tuple(set(update[update["event"] == "update"].id))  # 获取ids tuple
                sql = "select * from {}.{} where id in {}".format(mysql_common_database,mysql_label_reminder_table,id_tuple)  # SQL

                update_df= self.read.select_from_table(sql)  # 读取数据库获取
                history_df  = history_df [~history_df.id.isin(id_tuple)]
                changed_df = pd.concat([history_df, update_df], axis=0).reset_index().drop_duplicates()

                self.history_df = changed_df

            if len(delete) != 0:  # 删除相关数据
                history_df = self.history_df
                id_list = list(set(delete[delete["event"] == "update"].id))
                changed_df = history_df[~history_df.id.isin(id_list)]

                self.history_df = changed_df

        print("更新数据df耗时：%s" % (time.time() - start1))

    def label_match_reminder(self,label_list, history_df, user_id = None, label_name_var = "LabelName", user_id_var = "UserId"):
        """
        进行label与待提醒的label的匹配
        :param label:
        :param history_df: 从数据库导出的数据，或者经过等过更新的dataframe
        :param user_id:
        :return:
        """
        start2 = time.time()
        # 1. 进行label的匹配
        if user_id is not None:
            match_result_dataframe = history_df.loc[
                history_df[label_name_var].isin(label_list) & history_df[user_id_var] == user_id]
        else:
            match_result_dataframe = history_df.loc[history_df[label_name_var].isin(label_list)]

        # 2. 获取匹配结果 list of dict
        columns = ["id", "UserId", label_name_var, "RemindContent", "IsValid", "YtCode"]
        # match_result_dataframe = match_result_dataframe.rename(columns = {"id":"remind_LabelId"})

        remind_list = match_result_dataframe.to_dict(orient="records")

        print("标签匹配耗时：%s" % (time.time() - start2))
        return remind_list

    def reminder_result2_mysql(self,df):
        """
        将dataframe存入mysql
        :param df:
        :return:
        """

        def before_insert_mysql(self, dataframe, fill_string_vars=["YtCode"]):
            """进行插入mysql之前的处理，防止null"""
            # 1. 填充空字符串的列
            for col in fill_string_vars:
                dataframe[col] = dataframe[col].fillna("")

            # 2. 转 np.NaN 为 None，防止插入数据库报null错误
            insert_df = dataframe.astype(object).where(pd.notnull(dataframe), None)
            insert_df.reset_index(drop=True, inplace=True)

            # 3. 得到插入数据库的数据
            return insert_df
        label_sort_insert = before_insert_mysql(df)

        self.insert.insert_data_multi(label_sort_insert, database_name = mysql_common_database, table_name = mysql_label_reminder_table)
        print("话术提示结果，存入数据库完毕")

    def savedata(self, dic,table_name = mysql_label_reminder_table):
        """
        将dict插入数据库
        :param dic:
        :param table_name:
        :return:
        """
        self.insert.makepool(mysql_common_database)
        conn = self.insert.getconn()

        for i in list(dic.keys()):
            if dic[i] == None:
                dic.pop(i)

        sql1 = "INSERT INTO %s %s " % (table_name, tuple(dic.keys()))
        sql1 = sql1.replace("'","")
        sql2 = "VALUES {}".format(tuple(dic.values()))
        sql = sql1+sql2

        print("执行的sql,",sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def parsing_asr_dict(self,asr_dict):
        """
        解析通话标签字典
        :param asr_dict:
        :return:
        """
        # 1. 解析 asr_dict
        pid_var = "pid"
        unique_id_var = "unique_id"
        RegexId_var = "LabelId"
        QuetionLabel_var = "QuestionLabel"
        QuestionMatchText_var = 'QuestionMatchText'
        QuestionOriginalText_var = "QuestionOriginalText"
        AnswerLabel_var = "AnswerLabel"
        AnswerMatchText_var = 'AnswerMatchText'
        AnswerOriginalText_var = 'AnswerOriginalText'

        pid = asr_dict.get(pid_var)
        unique_id = asr_dict.get(unique_id_var)
        RegexId = asr_dict.get(RegexId_var)
        QuetionLabel = asr_dict.get(QuetionLabel_var)
        QuestionOriginalText = asr_dict.get(QuestionOriginalText_var)
        QuestionMatchText = asr_dict.get(QuestionMatchText_var)
        AnswerOriginalText = asr_dict.get(AnswerOriginalText_var)
        AnswerMatchText = asr_dict.get(AnswerMatchText_var)
        AnswerLabel = asr_dict.get(AnswerLabel_var)

        # 2. 获取待存入数据库的字典
        result_dict = {}
        result_dict[pid_var] = pid
        result_dict[unique_id_var] = unique_id
        result_dict["RegexId"] = RegexId
        print("正则id,",RegexId)
        result_dict[QuetionLabel_var] = QuetionLabel
        result_dict[QuestionMatchText_var] = QuestionMatchText
        result_dict[QuestionOriginalText_var] = QuestionOriginalText
        result_dict[AnswerLabel_var] = AnswerLabel
        result_dict[AnswerMatchText_var] = AnswerMatchText
        result_dict[AnswerOriginalText_var] = AnswerOriginalText

        result_dict["LabelId"] = None
        result_dict["UserId"] = None
        result_dict["LabelName"] = None
        result_dict["RemindContent"] = None
        return result_dict

    def get_reminder_content(self,asr_dict,label_var = "AnswerLabel"):
        """
        根据传过来的label，进行话术推荐
        :param asr_dict:
        :return:
        """
        # 1. 解析asr_dict
        result_dict = self.parsing_asr_dict(asr_dict)

        # 2. 进行匹配
        label = asr_dict.get(label_var)

        if (label is not None) & (label != ""):
            # 匹配标签
            remind_list = self.label_match_reminder(label_list = [label],history_df = self.history_df)

            print(remind_list)
            if len(remind_list) != 0:
                remind_dict = remind_list[0]
                print("匹配结果")
                print(remind_dict)

                result_dict["LabelId"] = remind_dict.get("id")
                result_dict["UserId"] = self.user_id
                result_dict["LabelName"] = remind_dict.get("LabelName")
                result_dict["RemindContent"] = remind_dict.get("RemindContent")

                # 存入数据库
                print(result_dict)
                self.savedata(dic = result_dict,table_name = mysql_label_reminder_result)
                return result_dict

                # 存入数据库

if __name__ == "__main__":
    asr_dict = {'pid': '1', 'unique_id': '0', 'LabelId': 11485, 'QuestionOriginalText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
                    'AnswerOriginalText': '嗯,你打错了吧', 'CallTime': '2020-02-02 02:02:02', 'CreateTime': '2020-09-03 16:16:05', 'decibel': 65, 'speech_speed': 25,
                    'talk_time': 2222, 'state': '0', 'QuestionLabel': '是否需要注销',
                    'QuestionMatchText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
                    'AnswerLabel': '打错了', 'AnswerMatchText': '嗯,你打错了吧'}

    start = time.time()
    get_remind = GetReminderContent()

    start = time.time()
    result_dict = get_remind.get_reminder_content(asr_dict = asr_dict)
    print(result_dict)
    print("耗时:%s"%(time.time()-start))



