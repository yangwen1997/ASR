#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: mongo.py
@time: 2020/4/30 2:35 下午
@desc:
'''

print("fadsf")
import sys
import os
print(os.environ['PUSHPATH'])
sys.path.append(os.environ['PUSHPATH'])
import pymongo
import pandas as pd
from base_class.config import *
from util.c_define import *
from util.mapDGG import *
from functools import reduce
import json


class ReadMongo(object):

    @staticmethod
    def read(table_name=None, columns=None, ignore_columns=None, filter_dict=None):
        # client = pymongo.MongoClient(host=mongo_host, port=mongo_port)
        client = pymongo.MongoClient(mongo_url)

        db = client[mongo_DB]

        # if mongo_user is not None and mongo_psw is not None:
        #     db.authenticate(mongo_user, mongo_psw)

        collection = db[table_name]  # 商务数据
        if filter_dict:
            print_to_log('传递了filter_dict: ', filter_dict)
            if columns:
                results = collection.find(filter_dict, {column: True for column in columns})
            elif ignore_columns:
                ignore_dict = {column: False for column in ignore_columns}
                print_to_log("ignore_dict", ignore_dict)
                results = collection.find(filter_dict, ignore_dict)
            else:
                results = collection.find(filter_dict, {})
        else:
            if columns:
                results = collection.find({}, {column: True for column in columns})
            elif ignore_columns:
                results = collection.find({}, {column: False for column in ignore_columns})
            else:
                raise ValueError('column_list和ignore_column_list必须有一个')

        try:
            return list(results)
        except Exception as e:
            
            print_to_log(e)

        return list()

    @staticmethod
    def bulk_read(table_name=None, key_name=None, values=None):
        # client = pymongo.MongoClient(host=mongo_host, port=mongo_port)\
        client = pymongo.MongoClient(mongo_url)
        db = client[mongo_DB]
        # if mongo_user is not None and mongo_psw is not None:
        #     db.authenticate(mongo_user, mongo_psw)
        collection = db[table_name]  # 商务数据
        print_to_log(key_name, values)
        return collection.find({key_name: {"$in": values}})


def read_business(p_code=smallyt_loans):
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        keyword = p_code
        condition = dict()
        condition['$regex'] = keyword
        filter_map["p_code"] = condition

        result = ReadMongo.read(table_name=mongo_busTable, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_business from mongo Error", level=5)

    return result


def read_business_with_division(division_ids=list()):
    try:
        # return ReadMongo.bulk_read(table_name=mongo_busTable, key_name="business_organization_id", values=division_ids)
        ignore_column_list = ['_id']
        results = [ReadMongo.read(table_name=mongo_busTable, filter_dict={"business_organization_id": str(division)},
                                  ignore_columns=ignore_column_list) for division in division_ids]
        result = None
        for ret in results:
            if result is None:
                result = ret
            else:
                result = result + ret
    except:
        result = None
        print_to_log("read_business from mongo Error", level=5)

    return result


def read_businessType(business_id=None):
    try:

        filter_map = dict()

        filter_map["business_id"] = business_id

        result = ReadMongo.read(columns=["p_code"], table_name=mongo_busTable, filter_dict=filter_map)
    except:
        result = None
        print_to_log("read_business from mongo Error", level=5)

    return result


def read_employee(p_code=smallyt_loans):
    try:
        ignore_column_list = ['_id']

        filter_map = dict()
        keyword = p_code
        condition = dict()
        condition['$regex'] = keyword
        filter_map["format_id"] = condition

        result = ReadMongo.read(table_name=mongo_empTable, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_employee from mongo Error", level=5)

    return result


def read_emp():
    try:
        ignore_column_list = ['_id']
        result = ReadMongo.read(table_name=mongo_empTable, ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_employee from mongo Error", level=5)

    return result


def read_employee_with_division(division_ids=list()):
    try:
        ignore_column_list = ['_id']
        results = [
            ReadMongo.read(table_name=mongo_empTable, filter_dict={"level_3": str(division)},
                           ignore_columns=ignore_column_list) for division in division_ids]
        result = None
        for ret in results:
            if result is None:
                result = ret
            else:
                result = result + ret
    except:
        result = None
        print_to_log("read_bustrain from mongo Error", level=5)
    return result


def read_train(p_code=smallyt_loans):
    try:
        ignore_column_list = ['_id']

        filter_map = dict()
        keyword = p_code
        condition = dict()
        condition['$regex'] = keyword
        filter_map["p_code"] = condition

        result = ReadMongo.read(table_name=mongo_trainTable, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_train from mongo Error", level=5)
    return result


def read_train_with_division(division_ids=list()):
    try:
        # return ReadMongo.bulk_read(table_name=mongo_trainTable, key_name="business_organization_id", values=division_ids)
        ignore_column_list = ['_id']
        # 多条件模糊查询
        # filter_map = {"business_organization_id": {"$in": division_ids}}
        # filter_map = {"$or": [{"business_organization_id": division} for division in division_ids]}
        # print("filter_map", filter_map)
        # result = ReadMongo.read(table_name=mongo_busTable, filter_dict=filter_map,
        #                         ignore_columns=ignore_column_list)
        results = [ReadMongo.read(table_name=mongo_trainTable, filter_dict={"business_organization_id": str(division)},
                                  ignore_columns=ignore_column_list) for division in division_ids]
        result = None
        for ret in results:
            if result is None:
                result = ret
            else:
                result = result + ret
    except:
        result = None
        print_to_log("read_train from mongo Error", level=5)
    return result


def read_division():
    try:
        ignore_column_list = ['_id']
        result = ReadMongo.read(table_name=mongo_divisionTable, ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_train from mongo Error", level=5)
    return result


def read_business_with_id(business_id=None):
    try:
        ignore_column_list = ['_id']
        result = ReadMongo.read(table_name=mongo_busTable, filter_dict={"business_id": business_id},
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_business from mongo Error", level=5)

    return result


def get_division_map():
    # 事业部ID: 业态编码
    map1 = dict()
    # 业态编码: list(事业部ID)
    map2 = dict()
    print_to_log("开始构造事业部-业态字典树")
    try:
        result = read_division()
        if len(result) < len(map1):
            print_to_log("无法更新事业部-业态字典树")
        else:
            for dic in result:
                division = dic['level_3']
                industry = dic['format_id']
                map1[division] = industry

                if industry not in map2.keys():
                    map2[industry] = list()
                division_list = map2[industry]
                division_list.append(division)
                map2[industry] = list(set(division_list))
    except Exception as e:
        print_to_log(e, level=5)
    return map1, map2


def read_bustrain(division_ids=list()):
    try:
        ignore_column_list = ['_id']
        results = [
            ReadMongo.read(table_name=mongo_bustrainTable, filter_dict={"business_organization_id": str(division)},
                           ignore_columns=ignore_column_list) for division in division_ids]
        result = None
        for ret in results:
            if result is None:
                result = ret
            else:
                result = result + ret
    except:
        result = None
        print_to_log("read_bustrain from mongo Error", level=5)
    return result


def read_busfeature(division_ids=list(), p_code=smallyt_loans):
    try:
        ignore_column_list = ['_id']
        results = [
            ReadMongo.read(table_name=mongo_busfeatureTable, filter_dict={"business_organization_id": str(division)},
                           ignore_columns=ignore_column_list) for division in division_ids]
        result = None
        for ret in results:
            if result is None:
                result = ret
            else:
                result = result + ret
    except:
        result = None
        print_to_log("read_busfeature from mongo Error", level=5)

    filter_map = dict()
    keyword = p_code
    condition = dict()
    condition['$regex'] = keyword
    filter_map["p_code"] = condition

    result_pcode = ReadMongo.read(table_name=mongo_busfeatureTable, filter_dict=filter_map,
                                  ignore_columns=ignore_column_list)
    union_all = result + result_pcode
    union = [dict(t) for t in set([tuple(d.items()) for d in union_all])]
    return union


def read_empfeature():
    try:
        ignore_column_list = ['_id']
        result = ReadMongo.read(table_name=mongo_empfeatureTable, ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_empfeature from mongo Error", level=5)
    return result


def read_new(p_code=smallyt_loans):
    try:
        ignore_column_list = ['_id']

        filter_map = dict()
        keyword = p_code
        condition = dict()
        condition['$regex'] = keyword
        filter_map["p_code"] = condition

        result = ReadMongo.read(table_name='train_new', filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_train from mongo Error", level=5)
    return result


def read_push(p_code=smallyt_loans):
    try:
        ignore_column_list = ['_id']

        filter_map = dict()
        keyword = p_code
        condition = dict()
        condition['$regex'] = keyword
        filter_map["p_code"] = condition

        result = ReadMongo.read(table_name='intelligent_train', filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("read_train from mongo Error", level=5)
    return result


def customer_call():
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_CALL_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("customer_call from mongo Error", level=5)
    return result


def customer_token():
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_CUSTOMERTOKENDF_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("customer_token from mongo Error", level=5)
    return result


def industry_token():
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_INDUSTRYTOKENDF_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("industry_token from mongo Error", level=5)
    return result


def customer_order():
    """
    读取 客户的订单次数和订单金额
    """
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_ORDER_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("customer_order from mongo Error", level=5)
    return result


def customer_order_date():
    """
    读取 客户的拜访次数和上门次数
    """
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_ORDERDATE_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("customer_order_date from mongo Error", level=5)
    return result


def customer_abandon():
    """
    读取 客户的拜访次数和上门次数
    """
    try:
        ignore_column_list = ['_id']
        filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_ABANDON_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("customer_abandon from mongo Error", level=5)
    return result


def customer_name(filter_map=dict()):
    try:
        ignore_column_list = ['_id']
        # filter_map = dict()
        result = ReadMongo.read(table_name=MONGODB_CUSTOMERNAME_COLLECTION, filter_dict=filter_map,
                                ignore_columns=ignore_column_list)
    except:
        result = None
        print_to_log("customer_name from mongo Error", level=5)
    return result


def write_MongoDB(df, mongo_url, db_name, table_name):
    # 1. 进行mongo连接
    client = pymongo.MongoClient(mongo_url)
    db = client[db_name]
    collection = db[table_name]

    # 2. 写入数据库
    data_json = json.loads(df.to_json(orient="records"))
    # collection.remove()# 删除原有数据
    collection.insert_many(data_json)# 插入数据


if __name__ == '__main__':
    import time
    import numpy as np

    start = time.time()
    from time import strftime, localtime


