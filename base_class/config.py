#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: database.py
@time: 2019-05-05 17:07
@desc: 用于存储数据常用的常量
'''
import sys
import os
from util.mapDGG import environment



def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class BaseConfig(object):
    def __init__(self):
        """徐志强、郭芳整理的数据仓---产线环境"""
        self.mongo_host = '172.16.0.25' if environment == 'DEBUG' else '10.2.1.121'
        self.mongo_port = 17017 if environment == 'DEBUG' else 17017
        self.mongo_database = 'warehouse' if environment == 'DEBUG' else None  #'warehouse'
        self.mongo_user = None if environment == 'DEBUG' else None #'sjznts'
        self.mongo_psw = None if environment == 'DEBUG' else None #'16IvgbuHTM'

        """潘进维护的MySQL"""

        self.DataWarehousing_host = '172.16.0.205' if environment == 'DEBUG' else '10.2.1.125'
        self.DataWarehousing_port = 3306 if environment == 'DEBUG' else 5506
        self.DataWarehousing_user = 'root' if environment == 'DEBUG' else 'db_data_warehouse'
        self.DataWarehousing_psw = 'root123456' if environment == 'DEBUG' else 'y0HTAw7S34d5jMt7'

        """数据中心---杨世杰---产线环境"""
        self.DataCenter_host = None
        self.DataCenter_port = None
        self.DataCenter_user = None
        self.DataCenter_psw = None

        kafka1 = ['192.168.254.80:9091', '192.168.254.80:9092', '192.168.254.80:9093']
        kafka2 = ['10.3.2.6:9091', '10.3.2.7:9091', '10.3.2.8:9091'] #普通系统
        kafka3 = ['10.0.0.134:9092','10.0.0.135:9092','10.0.0.136:9092'] #上市系统
        self.kafka_servers = kafka1 if environment == 'DEBUG' else kafka3
        # self.kafka_servers = kafka1 if environment == 'DEBUG' else kafka1


        # redis1 = [{'host': '192.168.254.131', 'port': 7001},
        #           {'host': '192.168.254.131', 'port': 7002},
        #           {'host': '192.168.254.131', 'port': 7003},
        #           {'host': '192.168.254.131', 'port': 7004},
        #           {'host': '192.168.254.131', 'port': 7005},
        #           {'host': '192.168.254.131', 'port': 7006}]

        # 如果没有密码，则去掉'password': 'ps2'这一部分即可
        redis1 = [{'host': '192.168.254.117', 'port': 7001},
                  {'host': '192.168.254.117', 'port': 7002},
                  {'host': '192.168.254.117', 'port': 7003},
                  {'host': '192.168.254.118', 'port': 7004},
                  {'host': '192.168.254.118', 'port': 7005},
                  {'host': '192.168.254.118', 'port': 7006}]

        redis2 = [{'host':'10.2.1.130', 'port': 7001},
                  {'host': '10.2.1.130', 'port': 7002},
                  {'host': '10.2.1.131', 'port': 7003},
                  {'host': '10.2.1.131', 'port': 7004},
                  {'host': '10.2.1.132', 'port': 7005},
                  {'host': '10.2.1.132', 'port': 7006}]

        self.redis_hosts = redis2 if environment == 'RELEASE' else redis1

mongo_host = BaseConfig().mongo_host
mongo_port = BaseConfig().mongo_port

#迁移服务器之后，使用mongo集群
# mongo1 = "mongodb://172.16.0.25:17017"
mongo1 = "mongodb://192.168.254.149:17017"
mongo2 = "mongodb://10.2.1.121:17017,10.2.1.122:17017,10.2.1.123:17017"
# mongo_url = mongo1 if environment == 'DEBUG' else mongo2
mongo_url = mongo2 if environment == 'DEBUG' else mongo2

# mysql -h 192.168.254.79 -P 5506 -u db_intelligent_customer -pdb_intelligent_customer
# 测试环境
if environment == "LOCAL":
    ConfigCustomerService_host = '192.168.254.18'
    ConfigCustomerService_port = 6666
    ConfigCustomerService_user = 'root'
    ConfigCustomerService_psw = 'mypass'
    """Redis测试地址"""
    redis_hosts = BaseConfig().redis_hosts
    QA_type = "test_question"
    QA_index = "dgg_cs_test_question"
    QA_database = [{"host": "192.168.254.126", "port": 9200},
                   {"host": "192.168.254.127", "port": 9200},
                   {"host": "192.168.254.128", "port": 9200}]
elif environment == "DEBUG":
    ConfigCustomerService_host = '192.168.254.79'
    ConfigCustomerService_port = 5506
    ConfigCustomerService_user = 'db_intelligent_customer'
    ConfigCustomerService_psw = 'db_intelligent_customer'
    QA_type = "question"
    QA_index = "dgg_cs_question"
    """Redis测试地址"""
    redis_hosts = BaseConfig().redis_hosts
    QA_database = [{"host": "192.168.254.126", "port": 9200},
                   {"host": "192.168.254.127", "port": 9200},
                   {"host": "192.168.254.128", "port": 9200}]
else:
    """Redis产线地址"""
    redis_hosts = [{'host': '10.0.0.140', 'port': 7001},
                   {'host': '10.0.0.140', 'port': 7002},
                   {'host': '10.0.0.141', 'port': 7003},
                   {'host': '10.0.0.141', 'port': 7004},
                   {'host': '10.2.1.163', 'port': 7005},
                   {'host': '10.2.1.163', 'port': 7006}]
    ConfigCustomerService_host = '10.2.1.210'
    ConfigCustomerService_port = 3309
    ConfigCustomerService_user = 'db_intelligent_customer'
    ConfigCustomerService_psw = '5tqbuq2Nx81lMQGC'
    QA_type = "question"
    QA_index = "dgg_cs_question"
    QA_database = [{"host": "10.0.0.140", "port": 9200},
                   {"host": "10.0.0.141", "port": 9200}]

mongo_database = BaseConfig().mongo_database
mongo_user = BaseConfig().mongo_user
mongo_psw = BaseConfig().mongo_psw
# 大数据预处理后的数据库
mongo_DB = 'model'
mongo_busTable = "business"
mongo_empTable = "employee"
mongo_trainTable = 'train'
mongo_divisionTable = 'division'

# 商机分类模型的数据表
# mongo_bustrainTable = "bus_train"
# mongo_busfeatureTable = "bus_feature"
# mongo_empfeatureTable = "empintelligent_feature"

mongo_bustrainTable = "business_train"
mongo_busfeatureTable = "business_new"
mongo_empfeatureTable = "empintelligent_feature"

# 跨业态所需要的数据---直接从Scala中的配置文件复制过来
MONGODB_VISIT_COLLECTION = "visit_count"
MONGODB_CALL_COLLECTION = "call"
MONGODB_ORDER_COLLECTION = "order"
MONGODB_ORDERDATE_COLLECTION = "order_date"
MONGODB_CUSTOMERNAME_COLLECTION = "customer_name"
MONGODB_INDUSTRYTOKENDF_COLLECTION = "industry_token"
MONGODB_CUSTOMERTOKENDF_COLLECTION = "customer_token"
MONGODB_ABANDON_COLLECTION = "abandon"

DataWarehousing_host = BaseConfig().DataWarehousing_host
DataWarehousing_port = BaseConfig().DataWarehousing_port
DataWarehousing_user = BaseConfig().DataWarehousing_user
DataWarehousing_psw = BaseConfig().DataWarehousing_psw

"""非上市系统"""
AllowAllotEmployee_host = "192.168.254.144" if environment == 'DEBUG' else "10.3.3.67"
AllowAllotEmployee_port = 3306 if environment == 'DEBUG' else 5506
AllowAllotEmployee_user = "root" if environment == 'DEBUG' else "db_iboss_allocat"
AllowAllotEmployee_psw = "root" if environment == 'DEBUG' else "db_iboss_allocat"
"""上市系统"""
# AllowAllotEmployee_host = "192.168.254.144" if environment == 'DEBUG' else "10.2.1.211"
# AllowAllotEmployee_port = 3306 if environment == 'DEBUG' else 5506
# AllowAllotEmployee_user = "root" if environment == 'DEBUG' else "db_iboss_allocat"
# AllowAllotEmployee_psw = "root" if environment == 'DEBUG' else "db_iboss_allocat"

"""ASR语音转文字数据存储地址"""
ASR_Mysql_host = "106.13.223.129" if environment == "DEBUG" else "106.13.223.129"
ASR_Mysql_port = 6033 if environment == 'DEBUG' else 6033
ASR_Mysql_user = "xdwh_test" if environment == "DEBUG" else "xdwh_test"
ASR_Mysql_psw = "xdwh_test" if environment == "DEBUG" else "xdwh_test"

"""网聊数据库地址"""
netchat_customer_service_hot = "10.2.1.210" if environment == "DEBUG" else "10.2.1.210"
netchat_customer_service_port = 3306 if environment == 'DEBUG' else 3306
netchat_customer_service_user = "chenxinswt" if environment == 'DEBUG' else "chenxinswt"
netchat_customer_service_psw = "chenxinswt" if environment == 'DEBUG' else "chenxinswt"

"""数据中心---杨世杰---产线环境"""
DataCenter_host = "10.2.1.127"
DataCenter_port = 5506
DataCenter_user = "ai_account"
DataCenter_psw = "qvufj2nj"

"""kafka地址"""
kafka_servers = BaseConfig().kafka_servers

"""kafka商机入topic"""
topic_in = 'ml_debug' if environment == 'DEBUG' else 'resourcealloc_smart'

"""kafka商机出topic"""
topic_out = 'resourcealloc_result'
topic_temp = 'temp_out'

"""智能推荐推荐客户入topic"""
# topic_in_recommended = "recommened_test"
topic_in_recommended = "busralloc_sync_resource_new_or_order_business"
"""智能推荐推荐客户出topic"""
# topic_out_recommended = "busralloc_sync_resource_new_or_order_business"
recommended_group_id = "iboss_recommend" #消费主

"""爬虫mongo数据库地址"""
Spider_mongo_host = "172.16.74.3"
Spider_mongo_port = 17017


"""Redis里商务队列"""
# redis：busralloc:sync:{areaCode}:{businessType}   areaCode为商机所属区域，businessType为商机所属业态

if __name__ == '__main__':
    print()
    print(BaseConfig().mongo_host)