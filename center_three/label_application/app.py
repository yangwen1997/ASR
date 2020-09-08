#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: app.py
@time: 2020/9/3 11:45 上午
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

import threading

