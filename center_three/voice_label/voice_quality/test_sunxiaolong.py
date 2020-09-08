#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: test_sunxiaolong.py
@time: 2020/8/14 2:15 下午
@desc:
'''
import os
import sys
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.voice_quality.data_processing_addfunc import Data_Processing
from center_three.voice_label.utils import Insert_Into_Mysql, table_data, Read_From_Mysql
from center_three.voice_label.config import *
import pymysql
import time
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime
import numpy as np
import pandas as pd


def pids():
    read = Read_From_Mysql()

    pids_df_1 = read.select_from_table(insert_database, "select distinct pid from q_yt_lable_result_202008")

    pids_df_2 = read.select_from_table(insert_database, "select distinct pid from q_yt_lable_unmatch_202008")

    pids_df = pd.concat([pids_df_1, pids_df_2], axis=0)

    pids_df.columns = ["pids"]
    pids_df.drop_duplicates(inplace=True)

    pids_df.reset_index(drop=True, inplace=True)

    import random

    pid_index = random.sample(set(pids_df.index), int(np.round(len(pids_df) / 20)))

    pids = pids_df.iloc[pid_index, :]
    pids = set(pids.pids)
    del read
    return pids

def test_main(pid, timestamp, type_code):
    insert_func = Insert_Into_Mysql(host='106.12.108.97',
    user ='root',
    password ="80390cb1a66549e1",
    port =3306)
    data_process = Data_Processing()
    data_process_debug = Data_Processing()

    data_process_debug.read = Read_From_Mysql(host='106.12.108.97',
    user ='root',
    password ="80390cb1a66549e1",
    port =3306)
    start = time.time()
    try:
        print("======================================我要开始提取语音标签了，pid:%s===================================="%pid)
        text_data = data_process.get_text_data(pid, timestamp)
        regex_df = data_process_debug.get_rex_df(type_code)  # 商务客户的正则
        customer_regex_df = data_process_debug.get_customer_regex_df(type_code) # 获取客户正则的df

        if len(regex_df) == 0:
            print("label库里没有数据")
        elif len(text_data) == 0:
            print("没有通话数据")
        elif len(customer_regex_df) == 0:
            print("客户文本正则没有数据")
        else:
            # TODO ==================================根据商务客户对话提取标签==================================
            # print("====================我要根据商务客户对话提取标签================")
            # match_all_df, match_key_df, no_match_df = data_process.match_regex(text_data, regex_df)
            # res_table_name = Res_Table.format(table_data(timestamp))
            # unmatch_table_name = Unmatch_Table.format(table_data(timestamp))
            #
            # if len(match_all_df) != 0:
            #     insert_func.flush_hosts(Res_Data_Base)
            #     try:
            #         insert_func.insert_data_multi(match_all_df, Res_Data_Base, res_table_name)
            #         print("存入匹配数据")
            #     except pymysql.ProgrammingError:
            #         insert_func.create_table(Res_Data_Base, res_table_name)
            #         insert_func.insert_data_multi(match_all_df, Res_Data_Base, res_table_name)
            #         print("建表{}存入匹配数据".format(res_table_name), data_process.table_data(time.time()))
            #
            # if len(match_key_df) != 0:
            #     insert_func.flush_hosts(Res_Data_Base)
            #     try:
            #         insert_func.insert_data_multi(match_key_df, Res_Data_Base,unmatch_table_name)
            #         print("存入未匹value配数据")
            #     except pymysql.ProgrammingError:
            #         insert_func.create_table(Res_Data_Base, unmatch_table_name)
            #         insert_func.insert_data_multi(match_key_df, Res_Data_Base, unmatch_table_name)
            #         print("建表{}存入未匹配数据".format(unmatch_table_name), data_process.table_data(time.time()))
            #
            # if len(no_match_df) != 0:
            #     insert_func.flush_hosts(Res_Data_Base)
            #     try:
            #         insert_func.insert_data_multi(no_match_df, Res_Data_Base, unmatch_table_name)
            #         print("存入未匹配数据")
            #     except pymysql.ProgrammingError:
            #         insert_func.create_table(Res_Data_Base, unmatch_table_name)
            #         insert_func.insert_data_multi(no_match_df, Res_Data_Base, unmatch_table_name)
            #         print("建表{}存入匹配数据".format(unmatch_table_name), data_process.table_data(time.time()))

            print("====================我要根据客户文本提取标签================")
            # customer_regex_df = data_process.get_customer_regex_df(type_code)  # 获取客户正则的df
            customer_match_all_df, customer_no_match_df, customer_result = data_process_debug.match_customer_label(text_data, customer_regex_df, pid, timestamp)
            customer_res_table_name = Customer_Res_Table.format(table_data(timestamp))
            # print("匹配成功的表，表名：%s"%customer_res_table_name)

            customer_unmatch_table_name = Customer_Unmatch_Table.format(table_data(timestamp))
            # print("未匹配成功的表，表名：%s" %customer_unmatch_table_name)

            if len(customer_match_all_df) != 0:
                insert_func.flush_hosts(Res_Data_Base)
                try:
                    insert_func.insert_data_multi(customer_match_all_df, Res_Data_Base, customer_res_table_name)
                    print("存入匹配数据")
                except pymysql.ProgrammingError:
                    print("没有匹配成功结果表,需要自己建表")
                    # print(customer_res_table_name)

                    insert_func.create_customer_label_table(Res_Data_Base, customer_res_table_name)
                    insert_func.insert_data_multi(customer_match_all_df, Res_Data_Base, customer_res_table_name)
                    print("建表{}存入匹配数据".format(customer_res_table_name), data_process.table_data(time.time()))

            if len(customer_no_match_df) != 0:
                insert_func.flush_hosts(Res_Data_Base)
                try:
                    insert_func.insert_data_multi(customer_no_match_df, Res_Data_Base, customer_unmatch_table_name)
                    print("存入匹配数据")
                except pymysql.ProgrammingError:
                    print("没有匹配未成功结果表,需要自己建表")
                    insert_func.create_customer_label_table(Res_Data_Base, customer_unmatch_table_name)
                    insert_func.insert_data_multi(customer_no_match_df, Res_Data_Base, customer_unmatch_table_name)
                    print("建表{}存入匹配数据".format(customer_unmatch_table_name), data_process.table_data(time.time()))
    except Exception as e:
        print(e)
    finally:
        print("耗时", time.time()-start)
        del insert_func
        del data_process
        del data_process_debug

def now_data(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->2020-07-01 10:10:10
    """
    dt = datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
    return date

def run():
    pid_set = pids()
    timestamp = time.time()
    type_code = ""
    for pis in pid_set:
        test_main(pis,timestamp,type_code)


if __name__ == '__main__':
    run()





