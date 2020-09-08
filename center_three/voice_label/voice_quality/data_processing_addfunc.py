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
from importlib import reload
from center_three.voice_label.voice_quality import test_func



class Data_Processing(object):



    def __init__(self):
        self.redis_sc = Redis_Pool(host=Redis_Hosts["host"], port=Redis_Hosts[
            "port"], max_connect=3, password=Redis_Hosts["password"])
        self.redis_conn = self.redis_sc.get_conn()
        self.read = Read_From_Mysql(host=Read_Data_Host, user=Read_Data_User,
                                    password=Read_Data_Password, port=Read_Data_Port)


    def get_id_time(self, key):
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
        SQL = "select * from {} where pid = '{}'".format(table_name, pid)
        DF = self.read.select_from_table(Data_Base, SQL)
        # 去空

        if len(DF) == 0:
            return pd.DataFrame()
        try:
            DF.columns = self.read.column_from_mysql(Data_Base,table_name)

        except Exception as e:
            print("库里没数据", e)

        DF = DF[DF.text.notnull()]
        if len(DF) == 0:
            return pd.DataFrame()
        # 按照创建时间排序
        DF.sort_values("start_time", inplace=True, ascending=True)
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
        DF_group.sort_values("sort_index", inplace=True, ascending=True)
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
                                     "question_result", "answer_result", "question_func", "answer_func"]]
                if type_code != "":
                    #TODO sid归类
                    type_df = DF[DF.yt_pcode == type_code]
                    if len(type_df) != 0:
                        type_df = type_df[["id", "sid", "yt_unique", "question_word",
                                          "answer_word", "question_result", "answer_result", "question_func", "answer_func"]]
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

    # 孙小龙
    def get_customer_regex_df(self, type_code):
        """
        获取客户的标签的正则dataframe
        :param type_code:
        :return:
        """

        table_name = Get_Regex_Table

        # SQL = """select * from {} where sid in
        #         (select id from {} where pid != 1 and deleted_at is null)
        #          and deleted_at is null and is_client = 1""".format(Get_Regex_Table, label_tabel_index)
        SQL = """select * from {} where deleted_at is null and is_client = 1""".format(Get_Regex_Table)

        # SQL = "select * from {} ".format(table_name)
        rd = self.read
        DF = rd.select_from_table(Res_Data_Base, SQL)

        if len(DF) != 0:
            DF.columns = rd.column_from_mysql(Res_Data_Base, table_name)
            DF = DF[DF.deleted_at.isnull()]

            # print("列名---待删除")
            # print(DF.columns)

            if len(DF) != 0:

                # TODO sid归类
                type_all = DF[DF.yt_pcode == ""]
                type_all = type_all[["id", "sid", "yt_unique", "question_word",
                                     "answer_word",
                                     "question_result", "answer_result","question_func"]]
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

    # ### 王辰的商务-客户匹配
    def  match_regex(self, data_df, regex_df):
        reload(test_func)
        func = test_func.Test_Func()
        """
        用data匹配所有regex 返回三个dataframe
        :param data_df:  data_dataframe
        :param regex_df: regex_dataframe
        :param pid: pid
        :param timestamp: 时间戳
        :return: 匹配key与value的dataframe/匹配key的dataframe/都没匹配到的dataframe
        """

        data_record = data_df.to_records(index=False)
        data_values = np.asarray(data_record)

        regex_record = regex_df.to_records(index=False)
        regex_values = np.asarray(regex_record)

        match_list = []
        notmatch_list = []
        for data in data_values:
            notmatch_dict = {}
            is_business = data.group_index.split("_")[1]
            if is_business == "1":
                continue
            bus_text = data["text"]
            bus_index = np.argwhere(data_values["text"] == bus_text)
            customer_index = bus_index + 1
            asr_qid = data["id"]
            pid = data["pid"]
            is_match = 0

            sids = set(regex_values["sid"])
            for sid in sids:
                value_dict = {}
                regex_array = regex_values[regex_values["sid"] == sid]
                for regex in regex_array:
                    question_regex = regex["question_word"]
                    question_label = regex["question_result"]
                    question_func = regex["question_func"]  # 暂时未使用
                    ylid = regex["id"]
                    try:
                        match_type = re.search(question_regex, bus_text)
                    except Exception as e:
                        print("Exception :", e)
                        print("可能错误的文本bus_text :", bus_text)
                        print("可能错误的正则question_regex :", question_regex)

                    if match_type != None:  # 匹配到key

                        # TODO: 读取问题的正则函数
                        if question_func != None:
                           question_func_result, question_func_type = getattr(func, question_func)(bus_text, match_type.group())
                           if question_func_result != None:
                               value_dict["question_result"] = question_func_result
                               value_dict["is_question_func"] = question_func
                               value_dict["asr_qid"] = asr_qid
                               value_dict["question_zresult"] = question_func_type
                               value_dict["question_original"] = bus_text
                               value_dict["sid"] = sid
                               value_dict["ylid"] = ylid
                               value_dict["pid"] = pid
                               is_match += 1
                        else:
                            value_dict["question_result"] = question_label
                            value_dict["is_question_func"] = ""
                            value_dict["asr_qid"] = asr_qid
                            value_dict["question_zresult"] = match_type.group()
                            value_dict["question_original"] = bus_text
                            value_dict["sid"] = sid
                            value_dict["ylid"] = ylid
                            value_dict["pid"] = pid
                            is_match += 1
                        try:
                            customer_array = data_values[customer_index].ravel()[0]
                            customer_text = customer_array["text"]
                            asr_aid = customer_array["id"]
                            value_dict["asr_aid"] = asr_aid
                            value_dict["answer_original"] = customer_text
                            answer_regex = regex["answer_word"]
                            answer_label = regex["answer_result"]
                            answer_func = regex["answer_func"]  # 暂时未使用
                            match_type2 = re.search(answer_regex, customer_text)
                            if match_type2 != None:
                                # TODO 读取回答的正则函数
                                if answer_func !=None:
                                    answer_func_result, answer_func_type = getattr(func, answer_func)(customer_text, match_type2.group())
                                    if answer_func_result != None:
                                        value_dict["answer_result"] = answer_func_result
                                        value_dict["is_ansewer_func"] = answer_func
                                        value_dict["answer_zresult"] = answer_func_type
                                        match_list.append(value_dict)
                                        break
                                else:
                                    value_dict["answer_result"] = answer_label
                                    value_dict["is_ansewer_func"] = ""
                                    match_answer = match_type2.group()
                                    value_dict["answer_zresult"] = match_answer
                                    match_list.append(value_dict)
                                    break

                        except:
                            pass
                        match_list.append(value_dict)
                        continue

            if is_match == 0:
                notmatch_dict["question_original"] = bus_text
                notmatch_dict["pid"] = pid
                notmatch_dict["asr_qid"] = asr_qid
                notmatch_dict["status"] = 0
                try:
                    customer_array = data_values[customer_index].ravel()[0]
                    asr_aid = customer_array["id"]
                    notmatch_dict["asr_aid"] = asr_aid
                    notmatch_dict["answer_zresult"] = customer_array["text"]
                except:
                    pass
                notmatch_list.append(notmatch_dict)
                continue

        match_df = pd.DataFrame(match_list)
        no_match_df = pd.DataFrame(notmatch_list)
        match_df.drop_duplicates(inplace=True)
        match_df["created_at"] = self.table_data(time.time())
        match_df.created_at = match_df.created_at.astype("str")
        no_match_df.drop_duplicates(inplace=True)
        no_match_df["created_at"] = self.table_data(time.time())
        no_match_df.created_at = no_match_df.created_at.astype("str")
        try:
            match_key_df = match_df[(match_df.answer_zresult.isnull()) & (match_df.asr_aid.notnull())]
            match_all_df = match_df[match_df.answer_zresult.notnull()]
            match_key_df.drop(["answer_zresult", "answer_result"], axis=1, inplace=True)
            # match_key_df.columns = ['asr_qid', 'question_zresult', 'question_result', 'question_original',
            #                         'sid', 'ylid', 'pid', 'asr_aid', 'answer_zresult', "created_at"]
        except:
            match_all_df = pd.DataFrame()
            match_key_df = pd.DataFrame()
        try:
            no_match_df = no_match_df[no_match_df.asr_aid.notnull()]
        except:
            no_match_df = pd.DataFrame()
        return match_all_df, match_key_df, no_match_df


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

    #### 孙小龙的客户问题的匹配
    def match_customer_label(self,data_df,regex_df,pid,timestamp):

        reload(test_func)
        func = test_func.Test_Func()

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
        question_func_name_list = [] # 正则函数名的列表

        question_category_list = regex_df["question_result"].unique().tolist() # 正则的所有问题的类别,并与类别中循环

        # 1. --------->进行模型识别
        # df["model_result"] = df["text"].apply(lambda x: self.model_predict(x))

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
                    # 某个问题类别下正则的行循环
                    md5 = getattr(regex_row, "yt_unique")
                    reg = getattr(regex_row, "question_word")
                    question = getattr(regex_row, "question_result")
                    sid = getattr(regex_row, "sid")
                    ylid = getattr(regex_row, "id")

                    question_func = getattr(regex_row, "question_func") # 暂时未使用

                    # 1. 进行正则提取
                    regex_result = re.search(r"%s" % reg, text_string)
                    if regex_result is None:
                        continue
                    else:
                        result = regex_result.group()
                        if result != "":

                            # 正则的提取结果不为空，再走函数提取
                            if question_func != None:
                                # 1. 进行正则函数的匹配，得到匹配结果
                                question_func_label,questionfunc_detail_result = getattr(func, question_func)(text_string,result)

                                if questionfunc_detail_result is None:
                                    continue
                                else:
                                    # question_func_result = getattr(func, question_func)(text_string)
                                    print("正则函数的提取结果,我在孙小龙的匹配函数match_customer_label里,用的正则函数名为：%s" % question_func)
                                    print(questionfunc_detail_result)

                                # question_func_result = getattr(func, question_func)(text_string)
                                # print("正则函数的提取结果,我在孙小龙的匹配函数match_customer_label里,用的正则函数名为：%s"%question_func)
                                # print(question_func_result)
                                # # 进行函数结果判断，是否为None
                                # if question_func_result is None:
                                #     continue
                                # else:
                                    result = questionfunc_detail_result

                                    question_func_name_list.append(question_func)
                                    sid_list.append(sid)
                                    ylid_list.append(ylid)
                                    customer_text_list.append(text_string)
                                    #                         text_id_list.append(getattr(row, "id"))
                                    regex_result_list.append(result)
                                    question_result_list.append(question_func_label) #现在去拿函数时，函数对应的类别可能为空
                            else:
                                result = result
                                question_func_name_list.append("")

                                sid_list.append(sid)
                                ylid_list.append(ylid)
                                customer_text_list.append(text_string)
                                #                         text_id_list.append(getattr(row, "id"))
                                regex_result_list.append(result)
                                if question is None:
                                    question_result_list.append("")
                                else:
                                    question_result_list.append(question)

                            # continue
                        else:
                            continue

        # 3. 正则结果成df
        #     regex_result_df = pd.DataFrame([sid_list,ylid_list,customer_text_list,text_id_list,regex_result_list,question_result_list]).T
        #     regex_result_df.columns = ["sid","ylid","原始问题文本","原始问题文本id","问法正则匹配结果","问法标签"]

        regex_result_df = pd.DataFrame(
            [sid_list, ylid_list, customer_text_list, question_func_name_list,regex_result_list, question_result_list]).T
        regex_result_df.columns = ["sid", "ylid", "原始问题文本","正则函数名","问法正则匹配结果", "问法标签"]
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
            result["正则函数名"] = ""

        result["asr_aid"] = np.NaN
        result["answer_result"] = np.NaN
        result["answer_zresult"] = np.NaN
        result["answer_original"] = np.NaN

        # 合并模型和正则结果
        #     result.loc[result["model_result"] != "未识别",result["问法标签"]] = result.loc[result["model_result"] != "未识别",result["model_result"]]

        # 5. 获取 match_all_df,match_key_df,no_match_df
        match_all_df = result.loc[result["原始问题文本"].notnull(), ["sid", "ylid", "pid", "id", "asr_aid",
                                                               "问法标签", "问法正则匹配结果", "text","正则函数名",
                                                               "answer_result", "answer_zresult", "answer_original"]]
        # ["sid","ylid","pid","id","asr_aid","问法标签","问法正则匹配结果","原始问题文本","答法标签","答法匹配结果","原始回答文本"]

        match_all_df.rename(columns={"问法标签": "question_result", "id": "asr_qid","正则函数名":"question_func_name",
                                     "问法正则匹配结果": "question_zresult", "text": "question_original"}, inplace=True)


        no_match_df = result.loc[result["原始问题文本"].isnull(), ["sid", "ylid", "pid", "id", "asr_aid",
                                                             "问法标签", "问法正则匹配结果", "answer_zresult",
                                                             "text","正则函数名"]]
        no_match_df["status"] = 0
        no_match_df.rename(columns={"问法标签": "question_result", "id": "asr_qid","正则函数名":"question_func_name",
                                    "问法正则匹配结果": "question_zresult", "text": "question_original"}, inplace=True)
        no_match_df.drop_duplicates(subset=["question_original", "asr_qid"], inplace=True)

        # 提取不为空的列,否则容易报错
        match_all_df = match_all_df[["sid", "ylid", "pid", "asr_qid","question_result", "question_zresult", "question_original","question_func_name"]]
        no_match_df = no_match_df[[ "pid", "asr_qid","question_original","question_func_name","status"]]


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