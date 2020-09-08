#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: business_kafka.py
@time: 2019-05-14 10:51
@desc:
'''
import sys
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import re
from kafka import KafkaConsumer
import numpy as np
import pandas as pd
# from property.recall.itemCF import ItemBasedCF

import time

sys.path.append(os.environ['PUSHPATH'])

from util.time import get_time_stamp13
from property.read_database.employee_redis import RedisTool
from property.offline.online_predict import rank
from property.read_database.kafka_producer import Producer
from base_class.config import topic_out, kafka_servers, topic_in
from util.c_define import *
from property.offline.lgb import DGGLightGBM


lgb_dgg = DGGLightGBM()
def read_from_kafka():
    """
    "allocStatus":"alloc_new",
    "areaCode":"BUS_SOR_PLACE_CD",
    "businessId":7807165398043648000,
    "businessType":"BUS_YT_DK",
    "createTime":1557477888000,
    "customerId":7717697800292667392,
    "customerNo":"KH201808142464",
    "id":4154,
    "noteDesc":"",
    "orderStatus":"UN_FLOW",
    "resAllocat":false,
    "resSign":0,
    "resSuccess":0,
    "typeAllocat":"B",
    “userId”:”分配结果需组装，分配到的商务Id”,
    “userNo”:”分配结果需组装，分配到的商务No”,
    “alloctTime”:”分配时间戳，分配结果需组装”
    """
    producer = Producer(topic=topic_out)
    consumer = KafkaConsumer(topic_in, group_id='ml', bootstrap_servers=kafka_servers, enable_auto_commit=False)

    print_to_log('consumer配置详情:', consumer.config)
    # threshold_values = sorted(np.random.uniform(low=0, high=0.1, size=100), reverse=True)

    recent500_A = pd.Series(np.random.uniform(low=0.5, high=0.6, size=500))
    recent500_B = pd.Series(np.random.uniform(low=0.1, high=0.7, size=500))
    # 每一期上线成功推送商机的个数
    push_count = 0
    for message in consumer:
        if message is not None:
            print_to_log("'Kafka Start consumer!'")
            start = time.time()
            value = str(message.value, encoding='utf-8')
            try:
                new_business = json.loads(value)
                customerId = new_business['customerId']
                print_to_log(new_business)
            except Exception as e:
                print_to_log(e, level=4)
                print_to_log('kafka解析出错', message, level=4)
                continue

            ranker = rank()
            if customerId == 'update_data':
                ranker.update_data()
            else:
                print_to_log('=====Start Pull Employee From Redis=====')
                employee_ids = []
                userId = []
                last_id = []
                try:
                    employees = RedisTool().get_employee_list().employees
                    for employee_dic in employees:
                        if int(employee_dic['userNo']) in ranker.user_list:
                            employee_ids.append(int(employee_dic['userNo']))
                            userId.append(int(employee_dic['userId']))
                            last_id.append(int(employee_dic['lastBusId']))
                    if len(employees) > 0:  # redis中有商务
                        if len(employee_ids) > 0:  # 离线模型中没有
                            print_to_log('End Pull Employee:', employee_ids)
                        else:
                            print_to_log('Redis中的商务列表没有特征，不做推荐')
                    else:
                        print_to_log('Redis中没有待推荐商务:', employees)

                except Exception as e:
                    print_to_log('error: ', e, '没有待选商务', level=4)

                # print_to_log('重复推荐,此商机上次被推荐的商务为: ', last_id)
                # 这是错误代码
                # for i in range(len(last_id)):
                #     if last_id[i] == customerId:
                #         last_id.remove(last_id[i])
                #         userId.remove(userId[i])
                #         employee_ids.remove(employee_ids[i])

                # 判断customerId是否在商机库中,同时待选商务数量是否大于0
                if len(employee_ids) > 0:
                    print_to_log('Start Prediction:')
                    try:
                        if push_count < 0:
                            user_probability = lgb_dgg.predict(bus_id=customerId, employee_list=employee_ids)
                            result = user_probability['user']
                            probability = float(user_probability['probability'])
                            new_business['noteDesc'] = "A:" + user_probability['desc']
                            recent500_A = recent500_A.append(pd.Series([float(probability)]), ignore_index=True)
                            if len(recent500_A) > 500:
                                recent500_A.drop(index=0, inplace=True)
                                recent500_A = recent500_A.reset_index()
                                recent500_A.drop(columns=['index'], inplace=True)
                        else:
                            user_probability = ranker.compute(bus_id=customerId, user_list=employee_ids)
                            result = user_probability['user']
                            probability = float(user_probability['probability'])
                            new_business['noteDesc'] = "B:" + user_probability['desc']
                            recent500_B = recent500_B.append(pd.Series([float(probability)]), ignore_index=True)
                            if len(recent500_B) > 500:
                                recent500_B = recent500_B.reset_index(drop=True)
                                recent500_B = recent500_B[1:]
                                # print(recent500_B.index)
                                # recent500_B.drop(index=0, inplace=True)
                                # recent500_B = recent500_B.reset_index()
                                # recent500_B.drop(columns=['index'], inplace=True)

                    except Exception as e:
                        print_to_log(e, level=5)
                        new_business['noteDesc'] = '出现bug不做推荐:%s' % str(e)

                    if result is not None:
                        # k = 20
                        # describe_series = recent500.describe(percentiles=[i / k for i in range(k + 1)])
                        # print(describe_series)
                        # threshold = describe_series['20%']
                        try:
                            if push_count < 0:
                                threshold = recent500_A.quantile(q=0.6)
                            else:
                                threshold = recent500_B.quantile(q=0.6)
                        except Exception as e:
                            print_to_log(e, level=5)
                            threshold = 0.8

                        if float(probability) > threshold:
                            new_business['userId'] = userId[employee_ids.index(result)]
                            new_business['userNo'] = result
                            new_business['alloctTime'] = str(get_time_stamp13())
                        else:
                            new_business['noteDesc'] = '最大概率:%s,低于阈值%s，不予推送' % (str(probability), str(threshold))
                else:
                    new_business['noteDesc'] = 'warning:商务列表employee_ids为空'

                print_to_log("返回给kafka的数据：")
                print_to_log(new_business)
                try:
                    producer.sendjsondata(json.dumps(new_business))
                except Exception as e:
                    print_to_log('producer.sendjsondata出错', e, level=5)
                print_to_log('总耗时:%.3f' % (time.time() - start))

        try:
            # time.sleep(30) # 用于复现 CommitFailedError: Commit cannot be completed since the group has already rebalanced and assigned the
            consumer.commit()
            print_to_log('Kafka consumer commit success!')
            push_count += 1
            print()  # 方便看日志记录
        except Exception as e:
            print_to_log('Kafka consumer commit failed!', e, level=5)
            print()


if __name__ == '__main__':
    read_from_kafka()
