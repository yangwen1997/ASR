#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: rating_data.py
@time: 2019-05-17 17:03
@desc: 用于加载全量数据
'''
from loans_drop.config.path import data_path
from util.c_define import *


class BaseLoader(object):
    """
    制定一些所有业态都需要遵守的规范，具体实现由各个子类负责
    """
    def __init__(self):
        self.total_data = None
        self.employee_df = None
        self.business_df = None
        today_date = datetime.datetime.now()
        self.date = today_date.strftime('%Y%m%d')
        # self.date = 20190704
        self.out_path = data_path + 'total_data{}.csv'.format(self.date)
        self.desc = 'This is BaseLoader, please overwrite this property'

    def pull_total_data(self):
        """
        加载历史数据
        :return:
        """
        raise NotImplementedError("为了实现所有业态的编程规范, 该方法必须实现")

    def update_model(self):
        """
        利用刚刚加载完的数据同时更新AB模型、特征工程
        :return:
        """
        raise NotImplementedError("为了实现所有业态的编程规范, 该方法必须实现")