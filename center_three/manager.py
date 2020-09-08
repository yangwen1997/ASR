#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: manager
@time: 2020/8/26 0026 13:48
@desc: 
'''



from util.c_define import *
from util.mapDGG import *
from base_class.config import topic_out


def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class Manager(object):
    def __init__(self):
        print_to_log("Start init...", str(self))
        self.receive_count = 0
        self.push_count = 0
        self.success_count = 0
        self.session_dict = dict()

    def update_data(self, is_init=False):
        """
        :param is_init: True 从init中传递过来， 其他场景调用此方法禁止传递此参数，否则导致重复训练
        :return:
        """
        print_to_log("Start update...", str(self))
        # 每一期收到kafka的商机个数
        self.receive_count = 0
        # 每一期成功发送到kafka商机个数
        self.push_count = 0
        # 每一期计算出大于阈值并且推送的商机个数
        self.success_count = 0


if __name__ == '__main__':

    import sys
    import os

    sys.path.append(os.environ['PUSHPATH'])