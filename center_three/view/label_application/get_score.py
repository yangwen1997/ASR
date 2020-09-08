#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: get_score.py
@time: 2020/9/3 2:16 下午
@desc:
'''
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from center_three.util.utils import *
from center_three.config.config import *
import time

class GetScore(object):
    """获取分数"""
    def __init__(self,user_id = None):

        self.asr_dict = asr_dict # 王辰语音传过来的数据
        self.user_id = user_id
        self.read = ReadFromMysql(mysql_host, mysql_user, mysql_password, mysql_port)  # 获取读取mysql连接池
        self.insert = InsertIntoMysql(mysql_host, mysql_user, mysql_password, mysql_port)  # 获取读取mysql连接池
        self.history_df = self.read_all_score_df()
        self.redis_pool = GetRedisClusterConn(RedisHosts, RedisPassWord)  # 创建redis_pool


    def read_all_score_df(self):
        """初始化时读取所有的评分规则"""
        self.read.makepool(mysql_common_database)

        # 1. sql
        if self.user_id is None:
            sql = "select * from {}.{} where {} = 1".format(mysql_common_database,mysql_score_table,"IsValid") # IsValid = 1,代表可用
        else:
            sql = "select * from {}.{} where {} = 1 and {} = {}".format(mysql_common_database,mysql_score_table,"IsValid","UserId",self.user_id) # IsValid = 1,代表可用

        rule_df = self.read.select_from_table(sql)
        rule_df.columns = self.read.column_from_mysql(mysql_score_table)

        return rule_df

    def update_rule_df(self):
        """从redis读取更新数据的id,并进行总体数据的更新"""
        # 1. 读取redis数据
        redis_conn = self.redis_pool.connect()  # 获取redis连接
        msg_list = redis_conn.lrange(mysql_score_table, 0, -1)  # 获取 redis某个key的 所有数据, ["{"action":"insert","id":1}"]
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
                sql = "select * from {}.{} where id in {}".format(mysql_common_database, mysql_score_table,ids)  # SQL
                insert_df = self.read.select_from_table(sql)  # 读取数据库获取
                changed_df = pd.concat([history_df, insert_df], axis=0).reset_index().drop_duplicates()  # 拼接去重

                self.history_df = changed_df

            if len(update) != 0:  # 获取更新数据
                history_df = self.history_df
                id_tuple = tuple(set(update[update["event"] == "update"].id))  # 获取ids tuple
                sql = "select * from {}.{} where id in {}".format(mysql_common_database,mysql_score_table,id_tuple)  # SQL

                update_df= self.read.select_from_table(sql)  # 读取数据库获取
                history_df  = history_df [~history_df.id.isin(id_tuple)]
                changed_df = pd.concat([history_df, update_df], axis=0).reset_index().drop_duplicates()

                self.history_df = changed_df

            if len(delete) != 0:  # 删除相关数据
                history_df = self.history_df
                id_list = list(set(delete[delete["event"] == "update"].id))
                changed_df = history_df[~history_df.id.isin(id_list)]

                self.history_df = changed_df

    def scoring(self,score_item_list, rule_df, total_score=100, user_id=None, is_continuous=0, score_name_var="ScoreName",
                user_id_var="UserId"):
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
            match_result_dataframe = rule_df.loc[
                rule_df[score_name_var].isin(score_item_list) & rule_df[user_id_var] == user_id]
        else:
            match_result_dataframe = rule_df.loc[rule_df[score_name_var].isin(score_item_list)]

        # print(match_result_dataframe.columns)
        match_result_dataframe["MinValue"] = match_result_dataframe["MinValue"].fillna("")
        match_result_dataframe["MaxValue"] = match_result_dataframe["MaxValue"].fillna("")
        match_result_dict = match_result_dataframe.to_dict(orient="records")
        # print(match_result_dict)
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
                score = dic.get("Score")
                weight = dic.get("ScoreWeight")
                min_value = dic.get("MinValue")
                max_value = dic.get("MaxValue")
                score_id = dic.get("id")

                # 构造数学区间

                if (min_value != "") & (max_value != ""):
                    # print(min_value)
                    # print(max_value)
                    pass

                elif (min_value == "") & (max_value == ""):
                    min_value = float("-inf")
                    # print(min_value)
                    # print(max_value)

                elif (min_value != "") & (max_value == ""):
                    max_value = float("inf")
                    # print(min_value)
                    # print(max_value)

                else:
                    continue

                if (value > min_value) & (value < max_value):
                    score_dict["score"] = score
                    score_dict["score_weight"] = weight
                    score_dict["ScoreId"] = score_id
                    break
                else:
                    continue
        else:
            for dic in match_result_dict:
                print(dic)

                score = dic.get("Score")
                weight = dic.get("ScoreWeight")
                score_id = dic.get("id")

                score_dict["ScoreId"] = score_id
                score_dict["score"] = score
                score_dict["score_weight"] = weight

        return score_dict

    def save_2_msyql(self,asr_dict, database_name , table_name):
        """
        df存数据库
        :param pid_list: 王辰的pid
        :param database_name:
        :param table_name:
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
        asr_dict = {'pid': '1', 'unique_id': '0', 'LabelId': 11485, 'QuestionOriginalText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
                    'AnswerOriginalText': '嗯,你打错了吧', 'CallTime': '2020-02-02 02:02:02', 'CreateTime': '2020-09-03 16:16:05', 'decibel': 65, 'speech_speed': 25,
                    'talk_time': 2222, 'state': '0', 'QuestionLabel': '是否需要注销',
                    'QuestionMatchText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
                    'AnswerLabel': '打错了', 'AnswerMatchText': '嗯,你打错了吧'}
        # 1. dict 转为 dataframe

        temp_df = pd.DataFrame([asr_dict])

        # 2. 插入数据库的对象

        # 3. 插入数据
        label_sort_insert = before_insert_mysql(temp_df)
        self.insert.insert_data_multi(label_sort_insert, database_name, table_name)

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

    def parsing_asr_dict(self, asr_dict):
        """
        解析通话标签字典
        :param asr_dict:
        :return:
        """
        # 1. 解析 asr_dict
        pid_var = "pid"
        unique_id_var = "unique_id"
        # RegexId_var = "LabelId"
        QuetionLabel_var = "QuestionLabel"
        QuestionMatchText_var = 'QuestionMatchText'
        QuestionOriginalText_var = "QuestionOriginalText"
        AnswerLabel_var = "AnswerLabel"
        AnswerMatchText_var = 'AnswerMatchText'
        AnswerOriginalText_var = 'AnswerOriginalText'

        pid = asr_dict.get(pid_var)
        unique_id = asr_dict.get(unique_id_var)
        # RegexId = asr_dict.get(RegexId_var)
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
        # result_dict["RegexId"] = RegexId
        # print("正则id,", RegexId)
        result_dict[QuetionLabel_var] = QuetionLabel
        result_dict[QuestionMatchText_var] = QuestionMatchText
        result_dict[QuestionOriginalText_var] = QuestionOriginalText
        result_dict[AnswerLabel_var] = AnswerLabel
        result_dict[AnswerMatchText_var] = AnswerMatchText
        result_dict[AnswerOriginalText_var] = AnswerOriginalText

        result_dict["ScoreId"] = None
        result_dict["UserId"] = None
        result_dict["ScoreName"] = None
        result_dict["ScoreValue"] = None
        result_dict["Score"] = None
        result_dict["ScoreWeight"] = None
        result_dict["TotalScore"] = None
        return result_dict

    def get_score(self,asr_dict,label_var = "QuestionLabel",is_continuous = 0): # ,label_var2 = 'decibel',label_var3 = 'speech_speed'
        """
        根据获取的解析数据，进行打分,并进行dict存数据库
        :param asr_dict:
        :param label_var:
        :return:
        """
        # 1. 解析asr_dict
        result_dict = self.parsing_asr_dict(asr_dict)

        # 2. 获取score字段项目
        value = asr_dict.get(label_var)

        def get_total_score(result_dict = None):
            """获取总分"""
            if result_dict is not None:
                result_dict["TotalScore"] = result_dict["score"] * result_dict["score_weight"]
                return result_dict
            else:
                return None

        if (value is not None) & (value != ""):
            score_dict = self.scoring(score_item_list=[{label_var: value}], rule_df= self.history_df, is_continuous = is_continuous)
            if score_dict is not None:
                score_dict = get_total_score(score_dict)

                result_dict["ScoreId"] = score_dict.get("ScoreId")
                result_dict["UserId"] = self.user_id
                result_dict["ScoreName"] = score_dict.get('score_item')
                result_dict["ScoreValue"] = score_dict.get("value")
                result_dict["Score"] = score_dict.get("score")
                result_dict["ScoreWeight"] = score_dict.get("score_weight")
                result_dict["TotalScore"] = score_dict.get("score") * score_dict.get("score_weight")

                # 存入数据库
                if result_dict["ScoreId"] is not None:
                    print(result_dict)
                    self.savedata(dic = result_dict,table_name = mysql_score_result)
                    print("评分成功，写入数据成功")
                else:
                    print("评分成功，写入数据成功")
                    print(result_dict)
                # return result_dict


    def get_all_score(self,asr_dict,label_dict_list):
        """
        获取多个评分
        :param asr_dict:
        :param label_dict_list:
        :return:
        """
        print(label_dict_list)
        for label_dict in label_dict_list:
            start = time.time()
            print(get_score.history_df[["ScoreName", "Score"]].head())
            name = label_dict.get("name")
            dic = self.scoring(score_item_list = [{name: asr_dict[name]}], rule_df = self.history_df,
                                    is_continuous = label_dict.get("is_continuous"))
            print(dic)
            print("耗时：%s" % (time.time() - start))

if __name__ == "__main__":
    asr_dict = {'pid': '1', 'unique_id': '0', 'LabelId': 11485, 'QuestionOriginalText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
                    'AnswerOriginalText': '嗯,你打错了吧', 'CallTime': '2020-02-02 02:02:02', 'CreateTime': '2020-09-03 16:16:05', 'decibel': 65, 'speech_speed': 25,
                    'talk_time': 2222, 'state': '0', 'QuestionLabel': '是否需要注销',
                    'QuestionMatchText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
                    'AnswerLabel': '打错了', 'AnswerMatchText': '嗯,你打错了吧'}
    # start = time.time()

    get_score = GetScore()

    label_dict1 = {"name":"QuestionLabel","is_continuous":0}
    label_dict2 = {"name":'decibel',"is_continuous":1}
    label_dict3 = {"name":'speech_speed',"is_continuous":1}

    label_dict_list = [label_dict1,label_dict2,label_dict3]

    get_score.get_all_score(asr_dict = asr_dict,label_dict_list = label_dict_list)





