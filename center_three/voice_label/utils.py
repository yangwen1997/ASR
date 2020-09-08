# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : wangchen
# @FILE     : utils.py
# @Time     : 2020/7/13 20:30
# @Software : PyCharm
import pandas as pd
from DBUtils.PooledDB import PooledDB
import pymysql
import os
import sys
from datetime import datetime
from rediscluster import RedisCluster
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.config import *
import redis
from pymongo import MongoClient
import json
from bson import ObjectId

class Read_From_Mysql(object):

    def __init__(self, host = Read_Data_Host, user = Read_Data_User,
                 password = Read_Data_Password, port = Read_Data_Port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def getmysqlservice(self, database):
        """
        :param database: 数据库
        :return: 相应库连接
        """
        pool = PooledDB(pymysql, 3, host=self.host,
                        user=self.user,
                        passwd=self.password,
                        db=database,
                        port=self.port,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def column_from_mysql(self, database_name, table_name):
        """
        :param database_name: 库名
        :param table_name: 表名
        :return: 表列名
        """
        conn = self.getmysqlservice(database_name)
        cs1 = conn.cursor()
        count2 = cs1.execute('SHOW FULL FIELDS FROM %s' % table_name)
        result2 = list(cs1.fetchall())
        columns = [column_set[0] for column_set in result2]
        cs1.close()
        conn.commit()
        conn.close()
        # print("查询到%d个特征:" % count2)
        return columns

    def select_from_table(self, database_name, SQL):
        """
        :param database_name: 库名
        :param SQL: sql
        :return: dataframe
        """
        conn = self.getmysqlservice(database_name)
        sc = conn.cursor()
        sc.execute(SQL)
        df = pd.DataFrame(list(sc.fetchall()))
        sc.close()
        conn.commit()
        conn.close()
        return df


class get_redis_cluster_conn(object):
    def __init__(self, startup_nodes = Redis_Hosts):
        self.hosts = startup_nodes
    def get_conn(self):
        """
        :return: redis 连接
        """
        try:
            redis_conn = RedisCluster(startup_nodes=self.hosts, decode_responses=True)
            return redis_conn
        except Exception as e:
            print("Redis Connect Error!")
            print("错误", e)
            return None

class MongoPool(object):
    """
    MongoDB连接池
    """
    def __init__(self, uri, max_conn=3):
        """
        :param max_conn: 最大连接数 默认3 必须是1到200之间的整数
        :param uri: 数据库连接uri mongodb://username:password@localhost:27017
        """

        # 如果max_conn为空或者不是1到200之间的整数 抛出异常
        if not max_conn or not isinstance(max_conn, int) or max_conn > 200 or max_conn < 1:
            raise TypeError(errorMsg='客官，max_conn不可以等于{}哦'.format(max_conn))
        self.__max_conn = max_conn
        self.__conn_list = []
        self.__uri = uri
        self.__idle = self.__max_conn
        self.__busy = 0
        self.__prepare_conn()

    def __prepare_conn(self):
        """
        根据参数max_conn初始化连接池
        :return: None
        """
        try:
            for x in range(0, self.__max_conn):
                conn_dict = {'conn': MongoClient(self.__uri), 'busy': False}
                self.__conn_list.append(conn_dict)
        except Exception as e:
            raise Exception('Bad uri: {}'.format(self.__uri))

    def get_conn(self):
        """
        从连接池中获取一个MongoDB连接对象
        :return: mongodb_connection
        """
        if self.__idle < 1:
            raise Exception(errorMsg='不好啦！Mongo的连接数不够用了！')
        for index in range(0, len(self.__conn_list)):
            conn = self.__conn_list[index]
            if conn.get('busy') == False:
                conn['busy'] = True
                self.__busy += 1
                self.__idle -= 1
                return conn.get('conn')

    def close(self, conn):
        """
        将参数中的连接池对象的busy改为False，标识此连接为空闲状态
        :param conn: mongoDB数据库连接对象
        :return: None
        """
        for index in range(0, len(self.__conn_list)):
            inner_conn = self.__conn_list[index]
            if inner_conn.get('conn') == conn:
                inner_conn['busy'] = True
                self.__busy -= 1
                self.__idle += 1
                inner_conn['busy'] = False
                break
            else:
                raise Exception("你特么的在逗我呢！这个连接不是从我这借的，我不要！")

    def get_set(self, conn, db, set_name):
        """
        获取集合连接
        :param conn: Mongo连接
        :param db: 数据库名
        :param set_name: 集合名
        :return: set_conn
        """
        set_conn = conn[db][set_name]

        return set_conn

    def updata(self, dataframe, db, set_name):
        df = dataframe.drop("user_id", axis=1)
        df = df.drop("created_at", axis=1)
        conn = self.get_conn()
        set_conn = self.get_set(conn, db, set_name)
        df._id = df._id.astype("str")
        df.reset_index(drop=True, inplace=True)
        update_value = json.loads(df.T.to_json()).values()
        for i in update_value:
            i["_id"] = ObjectId(i["_id"])
            try:
                set_conn.save(i)
            except Exception as e:
                print("更新数据失败", e, "源数据 : ", i)
        self.close(conn)



class Redis_Pool(object):
    def __init__(self, host, port, max_connect,password):
        self.host = host
        self.port = port
        self.max_connect = max_connect
        self.password = password
        self.pool = redis.ConnectionPool(host=self.host, port=self.port,
                                         max_connections=self.max_connect, password=self.password)

    def get_conn(self):
        conn = redis.Redis(connection_pool = self.pool,decode_responses=True)
        return conn


class Insert_Into_Mysql(object):
    def __init__(self, host = Insert_Data_Host, user = Insert_Data_User,
                 password = Insert_Data_Password, port = Insert_Data_Port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def getmysqlservice(self, database):
        """
        :param database: 库名
        :return: 连接
        """
        pool = PooledDB(pymysql, 3, host=self.host,
                        user=self.user,
                        passwd=self.password,
                        db=database,
                        port=self.port,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def flush_hosts(self, database):
        conn = self.getmysqlservice(database)
        cursor = conn.cursor()
        try:
            sql = "flush hosts"
            cursor.execute(sql)
            conn.commit()
        except:
            print("刷新host失败")
        finally:
            cursor.close()
            conn.close()

    def delete_from_table(self,database_name,table_name):
        """清空表中数据"""
        # 1. 创建mysqllain连接

        conn = self.getmysqlservice(database_name)
        cursor = conn.cursor()

        # 2. 进行数据表的清空
        SQL = 'delete from {}.{}'.format(database_name,table_name)
        print("表{}.{}清空成功".format(database_name,table_name))
        cursor.execute(SQL)
        conn.commit()
        cursor.close()
        conn.close()

    def insert_data_multi(self, dataframe, database, table_name):
        """插入多行数据"""
        # 先创建连接
        conn = self.getmysqlservice(database)
        cursor = conn.cursor()
        # 获取列名和值
        keys = dataframe.keys()
        # print("asr_qid在列名中么")
        # print(keys)
        values = dataframe.values.tolist()

        key_sql = ','.join(keys)
        value_sql = ','.join(['%s'] * dataframe.shape[1])
        SQL = 'insert into {}({}) values({})'.format(table_name, key_sql, value_sql)
        cursor.executemany(SQL, values)
        conn.commit()
        cursor.close()
        conn.close()

    def update_data_multi(self, dataframe, database, table_name):
        # 先创建连接
        conn = self.getmysqlservice(database)
        cursor = conn.cursor()
        num = len(dataframe)
        value = dataframe.values
        for i in range(num):
            if value[i, 2] != "":
                SQL = """update {}  set pid='{}' ,call_time ='{}',businessId ='{}',customerId ='{}',dmlabel = '{}',
                remark_result = '{}', status = '{}',remark_id = '{}',sid = '{}',ylid ='{}',remark_zresult ='{}',
                updated_at ='{}' where id = '{}'
                """.format(table_name, value[i, 1], value[i, 2], value[i, 3], value[i, 4], value[i, 5],
                           pymysql.escape_string(value[i, 7]), value[i, 8], value[i, 10], value[i, 11], value[i, 12],
                           pymysql.escape_string(value[i, 13]), value[i, 14], value[i, 0]).strip()
            else:
                SQL = """update {}  set pid='{}' ,businessId ='{}',customerId ='{}',dmlabel = '{}',
                               remark_result = '{}', status = '{}',remark_id = '{}',sid = '{}',ylid ='{}',remark_zresult ='{}',
                               updated_at ='{}' where id = '{}'
                               """.format(table_name, value[i, 1], value[i, 3], value[i, 4], value[i, 5],
                                          pymysql.escape_string(value[i, 7]), value[i, 8], value[i, 10], value[i, 11],
                                          value[i, 12],
                                          pymysql.escape_string(value[i, 13]), value[i, 14], value[i, 0]).strip()
            cursor.execute(SQL)
        conn.commit()
        cursor.close()
        conn.close()



# TODO 添加建表语句
    def create_table(self, database, table_name):
        # 先创建连接
        conn = self.getmysqlservice(database)
        cursor = conn.cursor()

        if "q_yt_lable_result" in table_name:
            sql = """CREATE TABLE `{}` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `sid` int(60) NOT NULL DEFAULT '0' COMMENT '标签分类表id 1,2,3',
                  `ylid` int(60) NOT NULL DEFAULT '0' COMMENT '标签表id 1,2,3',
                  `pid` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '通话ID',
                  `asr_qid` text COMMENT 'asr_list表问的id 1,2,3',
                  `asr_aid` text COMMENT 'asr_list表答的id 1,2,3',
                  `question_result` varchar(2000) DEFAULT NULL COMMENT '问法标签',
                  `question_zresult` varchar(2000) DEFAULT NULL COMMENT '问法正则匹配结果',
                  `question_original` varchar(2000) DEFAULT NULL COMMENT '问题原始文本',
                  `is_question_func` varchar(50) DEFAULT NULL COMMENT '问题函数名',
                  `answer_result` varchar(2000) DEFAULT NULL COMMENT '答法标签',
                  `answer_zresult` varchar(2000) DEFAULT NULL COMMENT '答法正则匹配结果',
                  `answer_original` varchar(2000) DEFAULT NULL COMMENT '原始回答文本',
                  `is_ansewer_func` varchar(50) DEFAULT NULL COMMENT '回答函数名',
                  `created_at` datetime DEFAULT NULL COMMENT '添加时间',
                  `updated_at` datetime DEFAULT NULL COMMENT '更新时间',
                  `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
                  PRIMARY KEY (`id`) USING BTREE,
                  KEY `pid` (`pid`) USING BTREE
                ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='通话标签结果表';
            """.format(
                table_name).strip()


        elif "q_cdr_followyc_" in table_name:
            sql = """CREATE TABLE `{}` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `businessId` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '商机id',
                  `customerId` bigint(20) NOT NULL COMMENT '客户id',
                  `pid` varchar(60) NOT NULL DEFAULT '' COMMENT '通话ID',
                  `type` tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '0没备注1有备注',
                  `created_at` datetime DEFAULT NULL COMMENT '添加时间',
                  `updated_at` datetime DEFAULT NULL COMMENT '更新时间',
                  PRIMARY KEY (`id`) USING BTREE,
                  KEY `pid` (`pid`) USING BTREE
                ) ENGINE=MyISAM AUTO_INCREMENT=28110 DEFAULT CHARSET=utf8 COMMENT='通话异常表';""".format(table_name).strip()

        elif "q_yt_lable_unmatch" in table_name:
            sql = """CREATE TABLE `{}` (
              `id` int(11) NOT NULL AUTO_INCREMENT,
              `sid` int(60) NOT NULL DEFAULT '0' COMMENT '标签分类表id 1,2,3',
              `ylid` int(60) NOT NULL DEFAULT '0' COMMENT '标签表id 1,2,3',
              `pid` varchar(60) NOT NULL DEFAULT '' COMMENT '通话ID',
              `asr_qid` text COMMENT 'asr_list表问的id 1,2,3',
              `asr_aid` text COMMENT 'asr_list表答的id 1,2,3',
              `question_result` varchar(2000) DEFAULT NULL COMMENT '问法标签(ylid有值才有)',
              `question_zresult` varchar(2000) DEFAULT NULL COMMENT '问法正则匹配结果(ylid有值存的是匹配结果否则存的是原始说的话)',
              `is_question_func` varchar(50) DEFAULT NULL COMMENT '问题函数名称',
              `answer_zresult` varchar(2000) DEFAULT NULL COMMENT '回答原句',
              `question_original` varchar(2000) DEFAULT NULL COMMENT '问题原句',
              `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0待学习1已学习2已忽略',
              `created_at` datetime DEFAULT NULL COMMENT '添加时间',
              `updated_at` datetime DEFAULT NULL COMMENT '更新时间',
              `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
              PRIMARY KEY (`id`) USING BTREE,
              KEY `pid` (`sid`) USING BTREE
            ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COMMENT='通话标签结果表';""".format(
                table_name).strip()

        else:
            sql = """
                        CREATE TABLE `{}` (
                  `id` int(11) NOT NULL AUTO_INCREMENT,
                  `sid` int(11) NOT NULL DEFAULT '0' COMMENT '标签分类表id ',
                  `ylid` int(11) NOT NULL DEFAULT '0' COMMENT '标签表id',
                  `businessId` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '商机id',
                  `customerId` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '客户id',
                  `remark_id` varchar(60) DEFAULT '0' COMMENT '商机备注id',
                  `pid` varchar(60) NOT NULL DEFAULT '' COMMENT '通话ID',
                  `dmlabel` varchar(100) DEFAULT '' COMMENT '标签',
                  `remark_result` varchar(2000) DEFAULT NULL COMMENT '备注原文',
                  `remark_zresult` varchar(2000) DEFAULT NULL COMMENT '备注正则结果',
                  `match_degree` float(5,2) DEFAULT NULL COMMENT '匹配度',
                  `status` tinyint(4) unsigned NOT NULL DEFAULT '0' COMMENT '0待质检1已质检',
                  `user_id` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT 'iboss用户id',
                  `call_time` datetime DEFAULT NULL COMMENT '通话开始时间',
                  `created_at` datetime DEFAULT NULL COMMENT '添加时间',
                  `updated_at` datetime DEFAULT NULL COMMENT '更新时间',
                  PRIMARY KEY (`id`) USING BTREE,
                  KEY `pid` (`pid`) USING BTREE
                ) ENGINE=MyISAM AUTO_INCREMENT=47480 DEFAULT CHARSET=utf8 COMMENT='通话备注质检结果';""".format(table_name).strip()

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()


## TODO 添加建立客户文本标签的存储表
    def create_customer_label_table(self, database, table_name):
        # 先创建连接
        conn = self.getmysqlservice(database)
        cursor = conn.cursor()

        if "customer_label_match" in table_name:
            sql = """
                CREATE TABLE `{}` (
              `id` int(11) NOT NULL AUTO_INCREMENT,
              `sid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '标签分类表id',
              `ylid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '标签表id',
              `pid` varchar(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '通话ID',
              `asr_qid` text DEFAULT NULL COMMENT 'asr_list表问的id[1,2,3]',
              `asr_aid` text DEFAULT NULL COMMENT 'asr_list表答的id[1,2,3]',
              `question_result` varchar(2000) DEFAULT NULL COMMENT '问法标签',
              `question_zresult` varchar(2000) DEFAULT NULL COMMENT '问法正则匹配结果',
              `question_original` varchar(2000) DEFAULT NULL COMMENT '原始问题文本',
              `question_func_name` varchar(2000) DEFAULT NULL COMMENT '正则函数名',
              `answer_result` varchar(2000) DEFAULT NULL COMMENT '答法标签',
              `answer_zresult` varchar(2000) DEFAULT NULL COMMENT '答法正则匹配结果',
              `answer_original` varchar(2000) DEFAULT NULL COMMENT '原始回答文本',
              `created_at` datetime DEFAULT NULL COMMENT '添加时间',
              `updated_at` datetime DEFAULT NULL COMMENT '更新时间',
              `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
              PRIMARY KEY (`id`) USING BTREE,
              KEY `pid` (`pid`) USING BTREE
            ) ENGINE=MyISAM AUTO_INCREMENT=15465 DEFAULT CHARSET=utf8 COMMENT='通话标签结果表';;
            """.format(
                table_name).strip()
            # print("没有匹配成功结果表，sql")
            # print(sql)

        # if "customer_label_unmatch" in table_name:
        else:
            sql = """CREATE TABLE `{}` (
              `id` int(11) NOT NULL AUTO_INCREMENT,
              `sid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '标签分类表id',
              `ylid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '标签表id',
              `pid` varchar(60) NOT NULL DEFAULT '' COMMENT '通话ID',
              `asr_qid` text DEFAULT NULL COMMENT 'asr_list表问的id[1,2,3]',
              `asr_aid` text DEFAULT NULL COMMENT 'asr_list表答的id[1,2,3]',
              `question_result` varchar(2000) DEFAULT NULL COMMENT '问法正则匹配结果(ylid有值才有)',
              `question_zresult` varchar(2000) DEFAULT NULL COMMENT '问的原句(ylid有值存的是匹配结果否则存的是原始说的话)',
              `answer_zresult` varchar(2000) DEFAULT NULL COMMENT '回答原句',
              `question_original` varchar(2000) DEFAULT NULL COMMENT '问题原句',
              `question_func_name` varchar(2000) DEFAULT NULL COMMENT '正则函数名',
              `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0待学习1已学习2已忽略',
              `created_at` datetime DEFAULT NULL COMMENT '添加时间',
              `updated_at` datetime DEFAULT NULL COMMENT '更新时间',
              `deleted_at` datetime DEFAULT NULL COMMENT '删除时间',
              PRIMARY KEY (`id`) USING BTREE,
              KEY `pid` (`sid`) USING BTREE
            ) ENGINE=MyISAM AUTO_INCREMENT=358116 DEFAULT CHARSET=utf8 COMMENT='通话标签结果表';""".format(
                table_name).strip()
            # print("没有匹配未成功结果表，sql")
            # print(sql)

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

def table_data(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->202007
    """
    dt = datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y%m')
    return date
