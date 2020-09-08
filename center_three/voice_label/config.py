# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : wangchen
# @FILE     : config.py
# @Time     : 2020/7/13 17:20
# @Software : PyCharm
import platform


environment = "normal"
# environment = "debug"



if environment == "debug":
    #外呼测试数据库地址
    debug_host = "106.12.108.97"
    debug_user = "root"
    debug_password = "80390cb1a66549e1"
    debug_port = 3306
    debug_regex_database = "ai_quilty"
    debug_Regex_Table = "q_yt_lable"


    #需语音质检数据mysql
    Read_Data_Host = "182.92.211.45"
    Read_Data_User = "root"
    Read_Data_Password = "dgg.root"
    Read_Data_Port = 3306
    Data_Base = "tcall_dgg_net"
    Res_Data_Base = "ai_quilty"
    Get_Text_Table = "asr_list_{}"
    Get_Regex_Table = "q_yt_lable"


    # inser_mysql
    Insert_Data_Host = "192.168.254.149"
    Insert_Data_User = "root"
    Insert_Data_Password = None
    Insert_Data_Port = 3306
    Regex_Data_Base = "ai_quilty"
    Res_Table = "q_yt_lable_result_{}"
    Unmatch_Table = "q_yt_lable_unmatch_{}"


    # 备注标签数据库与表
    call_createtime_database = "xdwh_test"
    call_createtime_table = "dgg_cdr_{}"
    insert_database = "ai_quilty"
    insert_table = "q_cdr_follow_{}"
    user_info_table = "q_sys_user_info"
    abnormal_table = "q_cdr_followyc_{}"

    # 客户文本标签表
    Customer_Res_Table = "customer_label_match_{}"
    Customer_Unmatch_Table = "customer_label_unmatch_{}"

    # iboss接口地址
    uri = "https://tibossuc.dgg188.cn/api/uc/customer/v1/find_customer_record_v2.do"
    unintention = '需要'

    label_tabel = "q_yt_lable"
    label_tabel_index = "q_yt_lable_sort"


    # redis 测试地址

    # Redis_Hosts = {'host': '127.0.0.1', 'port': 6379}
    Redis_Hosts = {'host': '123.56.120.105', 'port': 6379, "password": None}
    Redis_Key = "tag_push_list_key"
    sleep_time = 30  # redis 没有数据时候停顿时间

    # Mongourl -> mongodb: // username: password @ localhost:27017
    Mongouri = "mongodb://192.168.254.129:27017,192.168.254.130:27017,192.168.254.131:27017"
    MongoDataBase = "test_remark"
    MongoSet = "q_cdr_follow_test"

    # 用于test->regex
    if platform.system().lower() == 'windows':
        print("windows")
        test_data = "group_data\\CF_group.xlsx"  # pid	group_index	text
        test_regex = "regex\\CF_regex_W-增加办-0730.xlsx"  # SID	问题标签	问题正则	答案标签	答案正则	业态
        match_test_data = "result\\matchall_regex_result_{}.xlsx"
        match_key_data = "result\\match_key_regex_result_{}.xlsx"
        unmatch_test_data = "result\\unmatch_regex_result_{}.xlsx"
    elif platform.system().lower() == 'linux':
        print("linux")
        test_data = "group_data/CF_group.xlsx"  # pid	group_index	text
        test_regex = "regex/CF_regex_W.xlsx"  # SID	问题标签	问题正则	答案标签	答案正则	业态
        match_test_data = "result/matchall_regex_result_{}.xlsx"
        match_key_data = "result/matchall_regex_result_{}.xlsx"
        unmatch_test_data = "result/unmatch_regex_result_{}.xlsx"

else:
    # 需语音质检数据mysql
    Read_Data_Host = "182.92.211.45"
    Read_Data_User = "root"
    Read_Data_Password = "dgg.root"
    Read_Data_Port = 3306
    Data_Base = "tcall_dgg_net"
    Res_Data_Base = "ai_quilty"
    Get_Text_Table = "asr_list_{}"
    Get_Regex_Table = "q_yt_lable"
    label_tabel_index = "q_yt_lable_sort"

    # inser_mysql
    Insert_Data_Host = "182.92.211.45"
    Insert_Data_User = "root"
    Insert_Data_Password = "dgg.root"
    Insert_Data_Port = 3306
    Regex_Data_Base = "ai_quilty"
    Res_Table = "q_yt_lable_result_{}"
    Unmatch_Table = "q_yt_lable_unmatch_{}"

    # 备注标签数据库与表
    call_createtime_database = "tcall_dgg_net"
    call_createtime_table = "dgg_cdr_{}"
    insert_database = "ai_quilty"
    # insert_table = "q_cdr_follow_{}"
    user_info_table = "q_sys_user_info"
    abnormal_table = "q_cdr_followyc_{}"

    # 客户文本标签表
    Customer_Res_Table = "customer_label_match_{}"
    Customer_Unmatch_Table = "customer_label_unmatch_{}"

    # iboss接口地址
    uri = "https://ibossuc.dgg188.cn/api/uc/customer/v1/find_customer_record_v2.do"
    unintention = '有需求'

    # redis 测试地址
    Redis_Hosts = {'host': '123.56.120.105', 'port': 6379, "password": "dgg.root"}
    Redis_Key = "tag_push_list_key"
    sleep_time = 30  # redis 没有数据时候停顿时间

    # Mongourl -> mongodb: // username: password @ localhost:27017
    Mongouri = "mongodb://10.3.3.27:17017,10.3.3.28:17017,10.3.3.29:17017"
    # Mongouri = "mongodb://192.168.254.129:27017,192.168.254.130:27017,192.168.254.131:27017"
    # MongoDataBase = "test_remark"
    MongoDataBase = "ai_quilty"
    MongoSet = "q_cdr_follow"

# #用于test->regex
# test_data = "group_data\\CF_group.xlsx" #
# test_regex = "regex\\CF_regex_W.xlsx" #
# match_test_data = "result\\matchall_regex_result_{}.xlsx"
# unmatch_test_data = "result\\unmatch_regex_result_{}.xlsx"

#用于test->regex
test_data = "group_data/CF_group.xlsx" #
test_regex = "regex/CF_regex_W-增加办.xlsx" #
match_test_data = "result/matchall_regex_result_{}.xlsx"
unmatch_test_data = "result/unmatch_regex_result_{}.xlsx"
