# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : wangchen
# @FILE     : data_processing.py
# @Time     : 2020/7/13 21:00
# @Software : PyCharm
import os
import sys
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.utils import (Read_From_Mysql, Redis_Pool,
                                            table_data)
from center_three.voice_label.config import *
import time
import re
import pandas as pd
from datetime import datetime
import numpy as np


class Data_Processing(object):
    def __init__(self):
        self.redis_sc = Redis_Pool(host=Redis_Hosts["host"], port=Redis_Hosts[
            "port"], max_connect=3, password=Redis_Hosts["password"])
        self.redis_conn = self.redis_sc.get_conn()
        self.read = Read_From_Mysql(host = Read_Data_Host, user = Read_Data_User,
                                    password = Read_Data_Password, port = Read_Data_Port)

    def get_id_time(self,key):
        """
        :return: 获取redis中最老数据,返回dict
        """
        dc = self.redis_conn.lpop(key)
        if dc == None:
            print("redis没有数据,等我睡{}秒钟".format(sleep_time))
            print(self.table_data(time.time()))
            return None
        else:
            dc = eval(dc)
        return dc

    def get_text_data(self,pid,time):
        """
        :param id: 通话对应pid
        :param time: 年月 拼接表使用
        :return: dataframe
        """
        table_name = Get_Text_Table.format(table_data(time))
        SQL = "select * from {} where pid = '{}'".format(table_name,pid)
        DF = self.read.select_from_table(Data_Base, SQL)
        # 去空

        if len(DF) == 0:
            return pd.DataFrame()
        try:
            DF.columns = self.read.column_from_mysql(Data_Base,table_name)
        except Exception as e:
            print("库里没数据",e)

        DF = DF[DF.text.notnull()]
        if len(DF) == 0:
            return pd.DataFrame()
        # 按照创建时间排序
        DF.sort_values("start_time", inplace=True,ascending=False)
        DF = DF[["pid", "is_aleg", "text", "id"]]
        ls = self.create_index(DF)
        DF["group_index"] = ls
        DF["group_index"] = DF.apply(lambda x: str(x.group_index)+ "_"+str(x.is_aleg), axis = 1)
        DF.id = DF.id.astype("str")
        # 聚合每一通电话
        DF_group = DF.groupby(["pid", "group_index"]).agg(
            {'text': lambda x: ','.join(x), 'id': lambda x: ','.join(x)})
        # 修改列名
        DF_group.reset_index(inplace=True)
        DF_group.columns = ["pid", "group_index", "text","id"]
        DF_group["sort_index"] = DF_group.group_index.apply(
            lambda x: int(x.split("_")[0]))
        DF_group.sort_values("sort_index", inplace=True, ascending=False)
        DF_group = DF_group.reset_index(drop=True)
        return DF_group

    def create_index(self,DF):
        df_value = DF.values
        start = time.time()
        count_num = len(df_value)
        ls = []
        type_num = 0
        try:
            for i in range(count_num):
                if i == 0:
                    if df_value[i][1] == df_value[i + 1][1]:
                        ls.append(type_num)
                    else:
                        type_num += 1
                        ls.append(type_num)
                else:
                    if df_value[i][1] == df_value[i - 1][1]:
                        ls.append(type_num)
                    else:
                        type_num += 1
                        ls.append(type_num)
                    if i % 5000 == 0:
                        end_time = time.time()
                        print("耗时 : ", end_time - start)
                        print("构建group_index进度:", (i / count_num) * 100, "%")
            return ls
        except:
            return list(range(count_num))



    def get_rex_df(self,type_code):
        """取出各业态正则
        :param type_code: 业态code
        :return: regex->dataframe
        """
        #TODO 上线后或测试开始后可以删除这部分 从存入开始加密
        # def genearteMD5(str_data):
        #     """
        #     构建MD5
        #     :param str_data:
        #     :return:
        #     """
        #     # 创建md5对象
        #     hl = hashlib.md5()
        #
        #     # Tips
        #     # 此处必须声明encode
        #     # 否则报错为：hl.update(str)    Unicode-objects must be encoded before hashing
        #     hl.update(str_data.encode(encoding='utf-8'))
        #     return hl.hexdigest()

        table_name = Get_Regex_Table

        SQL = """select * from {} where sid in
                (select id from {} where pid != 1 and deleted_at is null)
                 and deleted_at is null and is_client = 0""".format(Get_Regex_Table,label_tabel_index)

        # SQL = "select * from {} ".format(table_name)
        rd = self.read
        DF = rd.select_from_table(Res_Data_Base, SQL)
        if len(DF) != 0:
            DF.columns = rd.column_from_mysql(Res_Data_Base, table_name)
            DF = DF[DF.deleted_at.isnull()]
            if len(DF) != 0:
                # DF.yt_unique = DF.apply(lambda x: x.question_result+x.yt_code+x.yt_pcode,
                #                         axis = 1)
                # DF.yt_unique = DF.yt_unique.apply(
                #     lambda x: genearteMD5(x))
                # TODO sid归类
                type_all = DF[DF.yt_pcode == ""]
                type_all = type_all[["id", "sid", "yt_unique", "question_word",
                                     "answer_word",
                                     "question_result", "answer_result"]]
                if type_code != "":
                    #TODO sid归类
                    type_df = DF[DF.yt_pcode == type_code]
                    if len(type_df) != 0:
                        type_df = type_df[["id", "sid", "yt_unique", "question_word",
                                          "answer_word", "question_result", "answer_result"]]
                        df = pd.concat([type_all, type_df])
                        df = df.reset_index(drop=True)
                    else:
                        df = type_all
                        df = df.reset_index(drop=True)
                else:
                    df = type_all
                    df = df.reset_index(drop=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        return df

    def get_customer_regex_df(self,type_code):
        """
        获取客户的标签的正则dataframe
        :param type_code:
        :return:
        """
        #TODO 上线后或测试开始后可以删除这部分 从存入开始加密
        # def genearteMD5(str_data):
        #     """
        #     构建MD5
        #     :param str_data:
        #     :return:
        #     """
        #     # 创建md5对象
        #     hl = hashlib.md5()
        #
        #     # Tips
        #     # 此处必须声明encode
        #     # 否则报错为：hl.update(str)    Unicode-objects must be encoded before hashing
        #     hl.update(str_data.encode(encoding='utf-8'))
        #     return hl.hexdigest()

        table_name = Get_Regex_Table

        SQL = """select * from {} where sid in
                (select id from {} where pid != 1 and deleted_at is null)
                 and deleted_at is null and is_client = 1""".format(Get_Regex_Table,label_tabel_index)

        # SQL = "select * from {} ".format(table_name)
        rd = self.read
        DF = rd.select_from_table(Res_Data_Base, SQL)
        if len(DF) != 0:
            DF.columns = rd.column_from_mysql(Res_Data_Base, table_name)
            DF = DF[DF.deleted_at.isnull()]
            if len(DF) != 0:
                # DF.yt_unique = DF.apply(lambda x: x.question_result+x.yt_code+x.yt_pcode,
                #                         axis = 1)
                # DF.yt_unique = DF.yt_unique.apply(
                #     lambda x: genearteMD5(x))
                # TODO sid归类
                type_all = DF[DF.yt_pcode == ""]
                type_all = type_all[["id", "sid", "yt_unique", "question_word",
                                     "answer_word",
                                     "question_result", "answer_result"]]
                if type_code != "":
                    #TODO sid归类
                    type_df = DF[(DF.yt_pcode == type_code) & (DF.sid != 52)]
                    if len(type_df) != 0:
                        type_df = type_df[["id", "sid", "yt_unique", "question_word",
                                          "answer_word", "question_result", "answer_result"]]
                        df = pd.concat([type_all, type_df])
                        df = df.reset_index(drop=True)
                    else:
                        df = type_all
                        df = df.reset_index(drop=True)
                else:
                    df = type_all
                    df = df.reset_index(drop=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        return df


    def match_regex(self,data_df,regex_df,pid,timestamp):
        """
        用data匹配所有regex 返回三个dataframe
        :param data_df:  data_dataframe
        :param regex_df: regex_dataframe
        :param pid: pid
        :param timestamp: 时间戳
        :return: 匹配key与value的dataframe/匹配key的dataframe/都没匹配到的dataframe
        """
        def drop_duplist(list_):
            """
            列表去重保持原排列顺顺
            :param list_:
            :return:
            """
            b = list(set(list_))
            b.sort(key=list_.index)
            return b


        to_be_Identify_list = []
        not_match_key = []
        num = data_df.shape[0]
        for i in range(num):  # 循环data表
            save_list = [[], [], [], [], [], [], [],
                         [], [], [], [], []]
            save_not_match = [[], [], [], []]
            # 标识,sid,question_result,question_zresult,data_key_id, answer_result,
            # answer_zresult, data_value_id, ylid, yt_unique 问题原文,回答原文
            text_i_1 = data_df.text.iloc[i]  # 商务的data_text
            text_id_1 = data_df.id.iloc[i]  # 商务data的id
            is_business = data_df.group_index.iloc[i].split("_")[1]  # 判断是否是商务说的话
            if is_business == "1":
                continue
            is_match = 0  # 用于判断是否有匹配到key
            for regex_i_key in range(len(regex_df)):  # 循环正则表
                id_ = regex_df.id.iloc[regex_i_key]  # 获得正则的id
                sid_ = regex_df.sid.iloc[regex_i_key]  # 获得正则的sid
                question_word_ = regex_df.question_word.iloc[regex_i_key]  # 获得key的正则
                question_res = regex_df.question_result.iloc[regex_i_key]  # key标签
                yt_unique_i = regex_df.yt_unique.iloc[regex_i_key]  # 标签类的yt_unique
                try:
                    match_type = re.search(str(question_word_), str(text_i_1))
                except Exception as e:
                    print("Exception :", e)
                    print("可能错误的文本text_i_1 :", text_i_1)
                    print("可能错误的正则question_word_ :", question_word_)
                    break
                if match_type != None:  # 匹配到key
                    question_result_match = match_type.group()
                    save_list[1].append(sid_)  # sid
                    save_list[2].append(question_res)  # key标签
                    save_list[3].append(question_result_match)  # 匹配结果
                    save_list[4].append(text_id_1)  # 商务的话
                    save_list[8].append(id_)  # 商务的话的id
                    save_list[9].append(yt_unique_i)  # 标签类的yt_unique
                    save_list[10].append(str(text_i_1))  # 问题原文

                    is_match += 1
            try:
                text_i_2 = data_df.text.iloc[i + 1]  # 客户的话
                text_id_2 = data_df.id.iloc[i + 1]  # 客户的话的id
                if is_match == 0:  # 都没匹配
                    save_not_match[0].append(text_i_1)  # 商务的话
                    save_not_match[1].append(text_id_1)  # 商务的话的id
                    save_not_match[2].append(text_i_2)  # 客户的话
                    save_not_match[3].append(text_id_2)  # 客户的话的id

                if len(save_list[9]) != 0:
                    save_list[11].append(str(text_i_2))
                    yt_unique_lsit = drop_duplist(save_list[9])
                    # yt_unique_lsit = list(set(save_list[9]))  # 去重
                    #
                    # yt_unique_lsit.sort(key=save_list[9].index)  # 按照原顺序

                    for yt_unique_id in yt_unique_lsit:
                        answer_word_list = regex_df[
                            regex_df.yt_unique == yt_unique_id].answer_word.values  # value正则_list
                        regex_label_list = regex_df[
                            regex_df.yt_unique == yt_unique_id].answer_result.values  # value标签_list
                        for regex_n in range(len(answer_word_list)):  # 循环类对应标签
                            regex = answer_word_list[regex_n]
                            regex_label = regex_label_list[regex_n]
                            match_type_2 = re.search(regex, text_i_2)
                            if match_type_2 != None:  # value匹配上了
                                match_answer = match_type_2.group()
                                save_list[5].append(regex_label)  # value标签
                                save_list[6].append(match_answer)  # value匹配的值
                                save_list[7].append(text_id_2)  # 客户的话的id
                                save_list[0].append(1)  # 匹配上了所有
                                break
                            else:
                                save_list[5].append("未匹配标签")
                                save_list[6].append(text_i_2)  # 客户的原话
                                save_list[7].append(text_id_2)  # 客户的话的id
                                save_list[0].append(0)  # 值匹配上了key
                    if len(save_list[0]) != 0:
                        to_be_Identify_list.append(save_list)
                if len(save_not_match[0]) != 0:
                    save_not_match[0] = ",".join(list(map(str, save_not_match[0])))
                    save_not_match[1] = ",".join(list(map(str, save_not_match[1])))
                    save_not_match[2] = ",".join(list(map(str, save_not_match[2])))
                    save_not_match[3] = ",".join(list(map(str, save_not_match[3])))
                    not_match_key.append(save_not_match)

            except:
                pass
                # print("最后一行啦")

        if len(to_be_Identify_list) != 0:
            match_all_df, match_key_df = self.save_list_2_dataframe(to_be_Identify_list, pid,
                                                            need_split=True)
        else:
            match_all_df, match_key_df = pd.DataFrame(),pd.DataFrame()
        if len(not_match_key) != 0:
            no_match_df = self.save_list_2_dataframe(not_match_key, pid, need_split=False)
        else:
            no_match_df = pd.DataFrame()

        return  match_all_df,match_key_df,no_match_df


    def save_list_2_dataframe(self,data_list, pid, need_split=True):
        """
        将匹配到key正则的list拆分成匹配到value 和为匹配到value 的dataframe
        或将未匹配key的list 转为dataframe
        :param data_list: 匹配到key的list (match_regex->to_be_Identify_list)
        :param pid: pid
        :param timestamp: 时间戳
        :param need_split: 如果为True是否需要切分 to_be_Identify_list
        如果为False-> 直接将未匹配key的list 转为dataframe (match_regex->not_match_key)
        :return:
        """
        if need_split == True:
            match_all = pd.DataFrame()
            match_key = pd.DataFrame()
            columns_list = ["match_code", "sid", "question_result",
                            "question_zresult", "asr_qid",
                            "answer_result", "answer_zresult", "asr_aid", "ylid", "useless", "Rqusetion_original", "answer_original"]
            for i in (data_list):
                df = pd.DataFrame(i[:]).T
                df.columns = columns_list
                df.drop("useless", axis=1, inplace=True)
                df = df[df.match_code.notnull()]
                df.fillna(method="ffill",inplace=True)
                df["pid"] = pid
                df["created_at"] = self.table_data(time.time())
                df["updated_at"] = self.table_data(time.time())
                match_all_i = df[df.match_code == 1]
                match_key_i = df[df.match_code == 0]
                match_all = pd.concat([match_all, match_all_i], axis=0)
                match_key = pd.concat([match_key, match_key_i], axis=0)
            match_all.drop('match_code', axis=1,inplace=True)
            match_key.drop(['match_code', "answer_result"], axis=1,inplace=True)
            match_all.drop_duplicates(inplace=True)
            match_key.drop_duplicates(inplace=True)
            match_key.drop("answer_original", axis=1, inplace=True)
            return match_all, match_key
        else:
            no_match = pd.DataFrame()
            columns_list = ["question_zresult", "asr_qid", "answer_zresult",
                            "asr_aid"]
            for i in (data_list):
                df = pd.DataFrame(i).T
                df.columns = columns_list
                df["pid"] = pid
                df["created_at"] = self.table_data(time.time())
                df["updated_at"] = self.table_data(time.time())
                no_match = pd.concat([no_match, df], axis=0)
            no_match.drop_duplicates(inplace=True)
            return no_match


    def table_data(self,timestamp):
        """
        :param timestamp:  时间戳
        :return: 年月 (格式)->202007
        """
        dt = datetime.fromtimestamp(int(timestamp))
        date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
        return date

    ## TODO ===========================我在模拟模型预测========================
    def model_predict(self,text):
        """利用模型预测类别"""

        if ("不需要" in text) | ("不用" in text):
            return "不需要"
        else:
            return "未识别"


    def match_customer_label(self,data_df,regex_df,pid,timestamp):
        """
        获取客户说话的label
        :param data_df:  data_dataframe
        :param regex_df: regex_dataframe
        :param pid: pid
        :param timestamp: 时间戳
        :return: 匹配出结果的dataframe/没匹配到的dataframe
        """
        # 1.提取客户说的话
        df = data_df[data_df["group_index"].str.contains("_1")]
        df["text"] = df["text"].astype(str)
        df["pid"] = df["pid"].astype(str)

        label_list = []

        customer_text_list = []  # 原文的list

        model_result_list = []  # 模型结果的list
        regex_result_list = []  # 正则匹配结果的list
        question_result_list = []  # 存储问题类别的list
        #     text_id_list = [] # 文本的id
        sid_list = []  # 问题类别id
        ylid_list = []  # 正则id

        question_category_list = regex_df["question_result"].unique().tolist()

        # 1. --------->进行模型识别
        df["model_result"] = df["text"].apply(lambda x: self.model_predict(x))

        # 2. 客户说话行循环匹配和模型识别
        for row in df.itertuples(index=True, name='Pandas'):
            # 提取客户的单句文本并相应添加list
            text_string = getattr(row, "text")
            #         customer_text_list.append(text_string)
            #         text_id_list.append(getattr(row, "id"))

            # 循环每个问题类别里的正则
            for question_category in question_category_list:
                # 提取某个问题类别下的所有正则的df
                one_question_regex_df = regex_df.loc[regex_df["question_result"] == question_category]

                for regex_row in one_question_regex_df.itertuples(index=True, name='Pandas'):
                    md5 = getattr(regex_row, "yt_unique")
                    reg = getattr(regex_row, "question_word")
                    question = getattr(regex_row, "question_result")
                    sid = getattr(regex_row, "sid")
                    ylid = getattr(regex_row, "id")

                    # 1. 进行正则提取
                    regex_result = re.search(r"%s" % reg, text_string)
                    if regex_result is None:
                        pass
                    else:
                        result = regex_result.group()
                        if result != "":
                            sid_list.append(sid)
                            ylid_list.append(ylid)
                            customer_text_list.append(text_string)
                            #                         text_id_list.append(getattr(row, "id"))
                            regex_result_list.append(result)
                            question_result_list.append(question)

                            continue
                        else:
                            pass

        # 3. 正则结果成df
        #     regex_result_df = pd.DataFrame([sid_list,ylid_list,customer_text_list,text_id_list,regex_result_list,question_result_list]).T
        #     regex_result_df.columns = ["sid","ylid","原始问题文本","原始问题文本id","问法正则匹配结果","问法标签"]

        regex_result_df = pd.DataFrame(
            [sid_list, ylid_list, customer_text_list, regex_result_list, question_result_list]).T
        regex_result_df.columns = ["sid", "ylid", "原始问题文本", "问法正则匹配结果", "问法标签"]
        regex_result_df["pid"] = pid

        # 4. 合并结果
        if len(regex_result_df) != 0:
            result = df.merge(regex_result_df, left_on=["text", "pid"], right_on=["原始问题文本", "pid"], how="left")
        else:
            result = df
            result["sid"] = np.NaN
            result["ylid"] = np.NaN
            result["原始问题文本"] = np.NaN
            result["问法正则匹配结果"] = np.NaN
            result["问法标签"] = np.NaN

        result["asr_aid"] = np.NaN
        result["answer_result"] = np.NaN
        result["answer_zresult"] = np.NaN
        result["answer_original"] = np.NaN

        # 合并模型和正则结果
        #     result.loc[result["model_result"] != "未识别",result["问法标签"]] = result.loc[result["model_result"] != "未识别",result["model_result"]]

        # 5. 获取 match_all_df,match_key_df,no_match_df
        match_all_df = result.loc[result["原始问题文本"].notnull(), ["sid", "ylid", "pid", "id", "asr_aid",
                                                               "问法标签", "问法正则匹配结果", "text",
                                                               "answer_result", "answer_zresult", "answer_original"]]
        # ["sid","ylid","pid","id","asr_aid","问法标签","问法正则匹配结果","原始问题文本","答法标签","答法匹配结果","原始回答文本"]

        match_all_df.rename(columns={"问法标签": "question_result", "id": "asr_qid",
                                     "问法正则匹配结果": "question_zresult", "text": "question_original"}, inplace=True)

        no_match_df = result.loc[result["原始问题文本"].isnull(), ["sid", "ylid", "pid", "id", "asr_aid",
                                                             "问法标签", "问法正则匹配结果", "answer_zresult",
                                                             "text"]]
        no_match_df["status"] = 0
        no_match_df.rename(columns={"问法标签": "question_result", "id": "asr_qid",
                                    "问法正则匹配结果": "question_zresult", "text": "question_original"}, inplace=True)
        no_match_df.drop_duplicates(subset=["qusetion_original", "asr_qid"], inplace=True)

        # 提取不为空的列,否则容易报错
        match_all_df = match_all_df[["sid", "ylid", "pid", "asr_qid","question_result", "question_zresult", "question_original"]]
        no_match_df = no_match_df[[ "pid", "asr_qid","qusetion_original","status"]]


        match_all_df["created_at"] = self.table_data(time.time())
        match_all_df["updated_at"] = self.table_data(time.time())
        no_match_df["created_at"] = self.table_data(time.time())
        no_match_df["updated_at"] = self.table_data(time.time())

        match_all_df["created_at"] = match_all_df["created_at"].astype(str)
        match_all_df["updated_at"] = match_all_df["updated_at"].astype(str)
        no_match_df["created_at"] = no_match_df["created_at"].astype(str)
        no_match_df["updated_at"] = no_match_df["updated_at"].astype(str)

        return match_all_df, no_match_df, result
        #     return result
    #