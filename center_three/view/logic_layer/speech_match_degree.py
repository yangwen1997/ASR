#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: speech_match_degree.py
@time: 2020/9/3 5:26 下午
@desc:
'''
import pandas as pd
import jieba

class MatchScore(object):
    """获取商务实际话术与提示话术的匹配度"""
    def __init__(self):
        self.stopwords_list = ["的","了","啊"]

    def drop_stopwords(self,word_list):
        """对分词结果去停用词"""
        after_drop_stopwords_list = [word for word in word_list if word in self.stopwords_list]

        return after_drop_stopwords_list

    def match_two_list(self,son_list,p_list):
        """比较两个list之间的匹配度"""
        # 1. 获取交集
        son_set = set(son_list)
        p_set = set(p_list)

        intersection_set = p_set.intersection(son_set)

        # 2. 获取交集占提示话术的百分比
        if (len(son_set) != 0) & (len(p_set) != 0):
            score = len(intersection_set) / len(p_set)
            score = round(score, 2)
            return score
        else:
            return None

    def get_match_score(self,saler_context,suggestive_context):
        """
        匹配商务实际文本 与 一个提示话术的匹配度
        :param saler_context: 一个商务实际文本
        :param suggestive_context: 一个提示话术文本
        :return:
        """
        # 1. 商务实际文本转list
        saler_context_word_list = jieba.lcut(saler_context)

        saler_context_word_list = self.drop_stopwords(saler_context_word_list)

        # 2. 提示话术文本转list
        suggestive_context_word_list = jieba.lcut(suggestive_context)

        suggestive_context_word_list = self.drop_stopwords(suggestive_context_word_list)

        # 3. 匹配得分
        match_score = self.match_two_list(son_list = saler_context_word_list,p_list = suggestive_context_word_list)

        return match_score

    def get_max_score(self,saler_context,all_suggestive_context_list):
        """
        获取商务实际文本 与 所有提示话术的匹配度
        :param saler_context:
        :param all_suggestive_context_list:
        :return:
        """
        saler_context_list = [saler_context] * len(all_suggestive_context_list)
        score_list = []

        for saler_t,suggestive_t in zip(saler_context_list,all_suggestive_context_list):
            temp_score = self.get_match_score(saler_context = saler_t,suggestive_context = suggestive_t)
            score_list.append(temp_score)

        max_score = max(score_list)
        max_score_index = score_list.index(max_score)
        max_suggestive_context = all_suggestive_context_list[max_score_index]

        result_dict = {"saler_context":saler_context,"matchest_suggestive_context":max_suggestive_context,"match_score":max_score}

        # df = pd.DataFrame(all_suggestive_context_list).T
        # df.columns = ["suggestive_context"]
        # # df["saler_context"] = saler_context
        # df["score"] = df["suggestive_context"].apply(lambda x: self.get_match_score(saler_context= saler_context,suggestive_context = x))

        return result_dict




