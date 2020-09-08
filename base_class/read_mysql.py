#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: read_mysql.py
@time: 2019-05-06 09:35
@desc: 基类，用于从mysql中读取数据
'''
from base_class.config import *
import pymysql
from DBUtils.PooledDB import PooledDB
from util.c_define import *


class Read_From_MySQL(object):
    def __init__(self, type = 0):
        self.type = type
        print_to_log('初始化connection')

    def getConnectionFromAllowAllotEmployee(self, database = None):
        """
        罗夕阳：从连接池获取连接对象，该方法是从DBUtils直接获取连接池，以后可能更换连接池
        Args:
            database: 数据库名字, 如'bi_behavior'
        Returns:
            connection对象
        """
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host=AllowAllotEmployee_host, user=AllowAllotEmployee_user, passwd=AllowAllotEmployee_psw,
                        db=database, port=AllowAllotEmployee_port, setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getConnectionFromDataCenter(self, database = None):
        """
        杨世杰：从连接池获取连接对象，该方法是从DBUtils直接获取连接池，以后可能更换连接池
        Args:
            database: 数据库名字, 如'bi_behavior'

        Returns:
            connection对象
        """
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host=DataCenter_host, user=DataCenter_user, passwd=DataCenter_psw,
                        db=database, port=DataCenter_port, setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getConnectionFromDataWarehousing(self, database = None):
        """
        徐志强：从连接池获取连接对象，该方法是从DBUtils直接获取连接池，以后可能更换连接池
        Args:
            database: 数据库名字, 如'bi_behavior'

        Returns:
            connection对象
        """
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host=DataWarehousing_host,
                        user=DataWarehousing_user,
                        passwd=DataWarehousing_psw,
                        db=database,
                        port=DataWarehousing_port,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getConnectionFromBigData(self, database = None):
        pool = PooledDB(pymysql, 3, host='192.168.254.147',
                        user='root',
                        passwd='root',
                        db=database,
                        port=3306,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def testCustomer_RecordFromIboss(self, database = None):
        """
        从iboss数据库中获取测试的生命周期记录
        Args:
            database: 数据库名字, 如'bi_behavior'

        Returns:
            connection对象
        """
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host='10.2.1.170',
                        user='ai_account',
                        passwd='qvufj2nj',
                        db=database,
                        port=5506,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getASRContent(self, database=None):
        """读取语音转文字"""
        pool = PooledDB(pymysql, 3, host=ASR_Mysql_host, user=ASR_Mysql_user, passwd=ASR_Mysql_psw,
                        db=database, port=ASR_Mysql_port, setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getConnectionFromPool(self, database=None):
        """
        从大数据仓获取数据
        Args:
            database: 数据库名字, 如'bi_behavior'

        Returns:
            connection对象
        """
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host='10.2.1.127',
                        user='ai_account',
                        passwd='qvufj2nj',
                        db=database,
                        port=5506,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getAllBusFrommysql(self):
        pool = PooledDB(pymysql, 3, host=AllowAllotEmployee_host,
                        user=AllowAllotEmployee_user,
                        passwd=AllowAllotEmployee_psw,
                        db= "tdgg_resource_alloc" if environment == "DEBUG" else "db_iboss_allocat",
                        port=AllowAllotEmployee_port,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def GetConnectionFromMiddleGround(self, database=None):
        """连接莫超data_middleground_subject，获取业绩表，跟进记录表"""
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host="192.168.254.144",
                        user="root", passwd="root",
                        db=database, port=3306, setsession=["SET AUTOCOMMIT = 1"])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getartificialintelligence(self, database=None):
        """
        推荐系统数据库读取数据
        Args:
            database: 数据库名字, 如'db_iboss_recommend'
        Returns:
            connection对象
        """
        # 3为连接池里的最少连接数，setsession=['SET AUTOCOMMIT = 1']是用来设置线程池是否打开自动更新的配置，0为False，1为True
        pool = PooledDB(pymysql, 3, host='192.168.254.79',
                        user='mycat',
                        passwd='IPY55QjNjv0I6e9t',
                        db=database,
                        port=5506,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getNetChatCustomerService(self,database):
        """
        获取网聊数据集
        :param database_name:
        :param table_name:
        :return:
        """
        pool = PooledDB(pymysql, 3, host=netchat_customer_service_hot,
                        user=netchat_customer_service_user,
                        passwd= netchat_customer_service_psw,
                        db= database,
                        port=netchat_customer_service_port,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def getCustomerServiceConfig(self,database):
        """
        获取智能客服后台管理系统的配置数据
        :param database_name:
        :param table_name:
        :return:
        """
        pool = PooledDB(pymysql, 3, host=ConfigCustomerService_host,
                        user=ConfigCustomerService_user,
                        passwd=ConfigCustomerService_psw,
                        db=database,
                        port=ConfigCustomerService_port,
                        setsession=['SET AUTOCOMMIT = 1'])
        conn = pool.connection()  # 以后每次需要数据库连接就是用connection（）函数获取连接就好了
        return conn

    def column_from_mysql(self, database_name, table_name):
        """
        获取数据库的特征列表
        Args:
            database_name: 数据库名字
            table_name: 数据表名字

        Returns:
            list = ['特征1',['特征2']。。。。]
        """

        if self.type == 0:
            conn = self.getConnectionFromDataWarehousing(database=database_name)
        elif self.type == 1:
            conn = self.getConnectionFromDataCenter(database=database_name)
        elif self.type == 2:
            conn = self.getConnectionFromBigData(database=database_name)
        elif self.type == 3:
            conn = self.getConnectionFromAllowAllotEmployee(database=database_name)
        elif self.type == 4:
            conn = self.getASRContent(database=database_name)
        elif self.type == 5:
            conn = self.GetConnectionFromMiddleGround(database=database_name)
        elif self.type == 6:
            conn = self.getNetChatCustomerService(database=database_name)
        else:
            raise Exception('目前只支持以上数据仓库', self.type)
        cs1 = conn.cursor()

        count2 = cs1.execute('SHOW FULL FIELDS FROM %s' % table_name)
        result2 = list(cs1.fetchall())
        columns = [column_set[0] for column_set in result2]

        cs1.close()
        conn.close()
        print_to_log("查询到%d个特征:" % count2)
        return columns

    def update_data(self):
        """更新dataframe中的数据到"""

if __name__ == "__main__":
    import sys
    sys.path.append(os.environ['PUSHPATH'])
    import pandas as pd
    a = Read_From_MySQL().getAllBusFrommysql()
    print("...")
    print(a)
