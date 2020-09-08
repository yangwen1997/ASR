#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: 
@file: map_tree.py
@time: 2019/9/19 0019 16:20
@desc: 存储中间数据的类，本来可以放入其他文件，但是容易引起高耦合，出现循环import的情况
文本语料下载：
https://www.meiwen.com.cn/subject/brjplqtx.html
https://www.cnblogs.com/wangyuxia/p/6667598.html
https://blog.csdn.net/wangyangzhizhou/article/details/78348949
'''
from base_class.read_mongo import get_division_map
from util.c_define import *
from util.mapDGG import log_path
import random
from sklearn.externals import joblib
import pickle
# clf=joblib.load('filename.pkl')
import pandas as pd


def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class Division(object):
    def __init__(self):
        self.division_industry = None
        self.industry_division = None
        self.update_division()
        self.business_origin_data = None


    def update_division(self):
        # 事业部ID: 业态编码 业态编码: list(事业部ID)
        self.division_industry, self.industry_division = get_division_map()
        print_to_log("industry_division : ", self.industry_division)
        print_to_log("division_industry: ", self.division_industry)
        # path = "/Users/sundali/Desktop/SunDaLi/Industrial_Projects/DGG/GIT/bss-recommended-platform/cross_industry/data/"
        # # pickle.load()
        # joblib.dump(self.industry_division,path+'业态编码到事业部id.pkl')
        # joblib.dump(self.division_industry,path+'事业部id到业态编码.pkl')
        # joblib.dump(all_docs, "all_docs")
        # self.division_industry = pd.DataFrame(self.division_industry)
        # self.industry_division = pd.DataFrame(self.industry_division)
        # self.division_industry.to_csv("事业部id到业态编码.csv",encoding="utf-8",index=0)
        # self.industry_division.to_csv( "/Users/sundali/Desktop/SunDaLi/Industrial_Projects/DGG/GIT/bss-recommended-platform/cross_industry/data/.csv",encoding="utf-8", index=0)

    def choice_employee(self, data=None, weight=None):
        """
        带权重随机,因为商务量小，选取第一种 https://www.cnblogs.com/zywscq/p/5469661.html
        :param data: 待选取序列
        :param weight: list对应的权重序列
        :return:选取的值
        """
        if len(data) == len(weight):
            new_list = []
            # for i in range(len(data)):
            #     val = data[i]
            for i, val in enumerate(data):
                new_list.extend([val] * weight[i])
            return random.choice(new_list)
        else:
            return None


if __name__ == "__main__":
    division = Division()
    division.update_division()
