# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : wangchen
# @FILE     : config.py
# @Time     : 2020/7/13 17:20
# @Software : PyCharm
import platform
# environment = "normal"
environment = "debug"

if environment == "debug":

    #  149测试地址
    mysql_host = "192.168.254.149"
    mysql_user = "root"
    mysql_password = None
    mysql_port = 3306
    mysql_common_database = "intelligent_seat"

    mysql_regex_table = "q_yt_lable"
    mysql_score_table = "score_table"
    mysql_score_result = "score_result"
    mysql_label_reminder_table = "label_reminder_table"
    mysql_label_reminder_result = "label_remind_result"
    mysql_label = "realtimelabel"


    #  数据库监听设置
    mysql_settings = {
        'host': '192.168.254.149',
        'port': 3306,
        'user': 'root',
        'passwd': None
    }

    #  redis 测试地址
    RedisHosts = [{'host': '192.168.254.117', 'port': 7001}, {'host': '192.168.254.117', 'port': 7002},
                  {'host': '192.168.254.117', 'port': 7003}, {'host': '192.168.254.118', 'port': 7004},
                  {'host': '192.168.254.118', 'port': 7005}, {'host': '192.168.254.118', 'port': 7006}]
    RedisPassWord = None

    #  Mongouri -> mongodb: // username: password @ localhost:27017
    Mongouri = "mongodb://192.168.254.129:27017,192.168.254.130:27017,192.168.254.131:27017"
    MongoDataBase = ""
    MongoSet = ""

    # 检查操作系统
    if platform.system().lower() == 'windows':
        print("windows")
    elif platform.system().lower() == 'linux':
        print("linux")


else:
    # 产线数据库地址
    mysql_host = "192.168.254.149"
    mysql_user = "root"
    mysql_password = None
    mysql_port = 3306
    mysql_common_database = "intelligent_seat"
    mysql_regex_table = "q_yt_lable"
    mysql_score_table = "score_table"
    mysql_label_reminder_table = "label_reminder_table"

    #  redis 测试地址
    RedisHosts = [{'host': '192.168.254.117', 'port': 7001}, {'host': '192.168.254.117', 'port': 7002},
                  {'host': '192.168.254.117', 'port': 7003}, {'host': '192.168.254.118', 'port': 7004},
                  {'host': '192.168.254.118', 'port': 7005}, {'host': '192.168.254.118', 'port': 7006}]
    RedisPassWord = None

    #  Mongouri -> mongodb: // username: password @ localhost:27017
    Mongouri = ""
    MongoDataBase = ""
    MongoSet = ""


