# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : wangchen
# @FILE     : run.py
# @Time     : 2020/7/13 20:55
# @Software : PyCharm
import os
import sys
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.voice_quality.data_processing_addfunc import Data_Processing
from center_three.voice_label.utils import Insert_Into_Mysql, table_data, MongoPool
from center_three.voice_label.config import *
from center_three.voice_label.remarks_quality.savebasicdata import SaveBasic
import pymysql
import time
import warnings
warnings.filterwarnings("ignore")
import json
from datetime import datetime


def main():
    insert_func = Insert_Into_Mysql()
    data_process = Data_Processing()
    mongo_pool = MongoPool(Mongouri)
    mongo_conn = mongo_pool.get_conn()
    start = time.time()
    try:
        # TODO redis 数据格式
        # 读出为一个json格式
        dc = data_process.get_id_time(Redis_Key)
        if dc != None:
            pid = dc["pid"]
            timestamp = dc["time"]
            type_code = dc["yt_code"]

            save_basic = SaveBasic()
            save_basic_df = save_basic.get_insert_data(pid, timestamp)

            if len(save_basic_df) != 0:
                set_conn = mongo_pool.get_set(mongo_conn, MongoDataBase, MongoSet)
                jsons = json.loads(save_basic_df.T.to_json())
                for json_num in jsons:
                    json_str = jsons[json_num]
                    try:
                        set_conn.insert(json_str)
                    except Exception as e:
                        print("插入访问接口基础数据错误", e, "数据为:", json_str)
                mongo_pool.close(mongo_conn)

           ################################# 以上存入备注数据 ################################
            print("======================================我要开始提取语音标签了，pid:%s===================================="%pid)
            voice_data = data_process.get_text_data(pid, timestamp)
            regex_df = data_process.get_rex_df(type_code)  # 商务客户的正则
            customer_regex_df = data_process.get_customer_regex_df(type_code) # 获取客户正则的df

            if len(regex_df) == 0:
                print("label库里没有数据")
            elif len(voice_data) == 0:
                print("没有通话数据")
            elif len(customer_regex_df) == 0:
                print("客户文本正则没有数据")
            else:
                print("====================我要根据商务客户对话提取标签================")
                match_all_df, match_key_df, no_match_df = data_process.match_regex(voice_data, regex_df)
                res_table_name = Res_Table.format(table_data(timestamp))
                unmatch_table_name = Unmatch_Table.format(table_data(timestamp))

                if len(match_all_df) != 0:
                    insert_func.flush_hosts(Res_Data_Base)
                    try:
                        insert_func.insert_data_multi(match_all_df, Res_Data_Base, res_table_name)
                        print("存入匹配数据")
                    except pymysql.ProgrammingError:
                        insert_func.create_table(Res_Data_Base, res_table_name)
                        insert_func.insert_data_multi(match_all_df, Res_Data_Base, res_table_name)
                        print("建表{}存入匹配数据".format(res_table_name), data_process.table_data(time.time()))

                if len(match_key_df) != 0:
                    insert_func.flush_hosts(Res_Data_Base)
                    try:
                        insert_func.insert_data_multi(match_key_df, Res_Data_Base,unmatch_table_name)
                        print("存入未匹value配数据")
                    except pymysql.ProgrammingError:
                        insert_func.create_table(Res_Data_Base, unmatch_table_name)
                        insert_func.insert_data_multi(match_key_df, Res_Data_Base, unmatch_table_name)
                        print("建表{}存入未匹配数据".format(unmatch_table_name), data_process.table_data(time.time()))

                if len(no_match_df) != 0:
                    insert_func.flush_hosts(Res_Data_Base)
                    try:
                        insert_func.insert_data_multi(no_match_df, Res_Data_Base, unmatch_table_name)
                        print("存入未匹配数据")
                    except pymysql.ProgrammingError:
                        insert_func.create_table(Res_Data_Base, unmatch_table_name)
                        insert_func.insert_data_multi(no_match_df, Res_Data_Base, unmatch_table_name)
                        print("建表{}存入匹配数据".format(unmatch_table_name), data_process.table_data(time.time()))

# ===============================================以下孙小龙========================================================================
                # TODO ==================================根据客户的文本提取标签==================================
                print("====================我要根据客户文本提取标签================")
                customer_match_all_df, customer_no_match_df, customer_result = data_process.match_customer_label(voice_data, customer_regex_df, pid, timestamp)
                customer_res_table_name = Customer_Res_Table.format(table_data(timestamp))
                customer_unmatch_table_name = Customer_Unmatch_Table.format(table_data(timestamp))

                if len(customer_match_all_df) != 0:
                    insert_func.flush_hosts(Res_Data_Base)
                    try:
                        insert_func.insert_data_multi(customer_match_all_df, Res_Data_Base, customer_res_table_name)
                        print("存入匹配数据")
                    except pymysql.ProgrammingError:
                        print("没有匹配成功结果表,需要自己建表")
                        print(customer_res_table_name)

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

        else:
            time.sleep(sleep_time)
    except Exception as e:
        print(e)
    finally:
        print("耗时",time.time()-start)
        del insert_func
        del data_process

def now_data(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->2020-07-01 10:10:10
    """
    dt = datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
    return date

if __name__ == '__main__':
    while 1:
        start = time.time()
        main()
        print("耗时:", time.time() - start, "当前:", now_data(time.time()))


