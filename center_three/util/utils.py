# !/usr/bin/env Python3
# -*- coding: utf-8 -*-
# @Author   : wangchen
# @FILE     : utils.py
# @Time     : 2020/7/13 20:30
# @Software : PyCharm
import pandas as pd
from DBUtils.PooledDB import PooledDB
import pymysql
from center_three.config.config import *
import redis
from pymongo import MongoClient
import json
from bson import ObjectId
import os
import sys
from rediscluster import StrictRedisCluster
sys.path.append(os.environ['PUSHPATH'])


class ReadFromMysql(object):
    def __init__(self, host=None, user=None,
                 password=None, port=None):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.pool = None

    def makepool(self, database):
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
        if self.pool == None:
            self.pool = pool
        else:
            self.pool.close()
            self.pool = pool

    def getconn(self):
        conn = self.pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def column_from_mysql(self, table_name):
        """
        :param database_name: 库名
        :param table_name: 表名
        :return: 表列名
        """
        conn = self.getconn()
        cs = conn.cursor()
        # count2 = cs1.execute('SHOW FULL FIELDS FROM %s' % table_name)
        count2 = cs.execute('SHOW FULL FIELDS FROM %s' % table_name)
        result2 = list(cs.fetchall())
        columns = [column_set[0] for column_set in result2]
        conn.commit()
        cs.close()
        conn.close()
        return columns

    def select_from_table(self, SQL):
        """
        :param database_name: 库名
        :param SQL: sql
        :return: dataframe
        """
        conn = self.getconn()
        sc = conn.cursor()
        sc.execute(SQL)
        df = pd.DataFrame(list(sc.fetchall()))
        conn.commit()
        sc.close()
        conn.close()
        return df


class GetRedisClusterConn(object):  # 连接redis集群

    def __init__(self, reidshost=None, redispasword=None):
        self.conn_list = reidshost  # 连接列表
        self.password = redispasword

    def connect(self):
        """
        连接redis集群
        :return: object
        """
        try:
            # 非密码连接redis集群
            # redisconn = StrictRedisCluster(startup_nodes=self.conn_list)
            # 使用密码连接redis集群
            redisconn = StrictRedisCluster(startup_nodes=self.conn_list, password=self.password, decode_responses=True)
            return redisconn
        except Exception as e:
            print(e)
            print("错误,连接redis 集群失败")
            return False


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


class RedisPool(object):
    """
    用于单节点redispool
    """
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


class InsertIntoMysql(object):
    def __init__(self, host=None, user=None,
                 password=None, port=None):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.pool = None

    def makepool(self, database):
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
        if self.pool == None:
            self.pool = pool
        else:
            self.pool.close()
            self.pool = pool

    def getconn(self):
        conn = self.pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def flush_hosts(self):
        conn = self.getconn()
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

    def delete_from_table(self, database_name, table_name):
        """清空表中数据"""
        # 1. 创建mysqllain连接

        conn = self.getconn()
        cursor = conn.cursor()

        # 2. 进行数据表的清空
        SQL = 'delete from {}.{}'.format(database_name, table_name)
        print("表{}.{}清空成功".format(database_name, table_name))
        cursor.execute(SQL)
        conn.commit()
        cursor.close()
        conn.close()

    def insert_data_multi(self, dataframe, table_name):
        """插入多行数据"""
        # 先创建连接
        conn = self.getconn()
        cursor = conn.cursor()
        # 获取列名和值
        keys = dataframe.keys()
        values = dataframe.values.tolist()
        key_sql = ','.join(keys)
        value_sql = ','.join(['%s'] * dataframe.shape[1])
        SQL = 'insert into {}({}) values({})'.format(table_name, key_sql, value_sql)
        cursor.executemany(SQL, values)
        conn.commit()
        cursor.close()
        conn.close()

    def update_data_multi(self):
        # 先创建连接
        conn = self.getconn()
        cursor = conn.cursor()
        sql = ""
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()




