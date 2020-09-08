#!/usr/bin/env python
# encoding: utf-8
'''
@author: 结巴源码 罗成 第一次修改
@contact: luocheng@dgg.net
@file: lgb_train.py
@time: 2019-07-10 15:49
@desc: 未登陆词的分词  简单的Viterbi算法 finalseg 模块中的__init__方法中，真正的Viterbi算法在posseg模块下也有Viterbi算法的详细代码

a. 评估问题(概率计算问题)
即给定观测序列 O=O1,O2,O3…Ot和模型参数λ=(A,B,π)，怎样有效计算这一观测序列出现的概率.
(Forward-backward算法)
b. 解码问题(预测问题)
即给定观测序列 O=O1,O2,O3…Ot和模型参数λ=(A,B,π)，怎样寻找满足这种观察序列意义上最优的隐含状态序列S。
(viterbi算法,近似算法)
c. 学习问题
即HMM的模型参数λ=(A,B,π)未知，如何求出这3个参数以使观测序列O=O1,O2,O3…Ot的概率尽可能的大.
(即用极大似然估计的方法估计参数,Baum-Welch,EM算法)
https://blog.csdn.net/gfsfg8545/category_5778483.html

模型的关键相应参数λ=(A,B,π)，经过作者对大量语料的训练, 得到了finalseg目录下的三个文件
    prob_start.py；（初始化状态概率（π）即词语以某种状态开头的概率，其实只有两种，要么是B，要么是S。这个就是起始向量, 就是HMM系统的最初模型状态
    prob_trans.py ；隐含状态概率转移矩A 即字的几种位置状态(BEMS)的转换概率
    prob_emit.py : 观测状态发射概率矩阵B 即位置状态到单字的发射概率，比如P(“狗”|M)表示一个词的中间出现”狗”这个字的概率
    这几个参数怎么得到的，具体方法见作者详述。https://github.com/fxsjy/jieba/issues/7
'''

from __future__ import absolute_import, unicode_literals
import re
import os
import sys
import pickle
from .._compat import *


MIN_FLOAT = -3.14e100

PROB_START_P = "prob_start.p"
PROB_TRANS_P = "prob_trans.p"
PROB_EMIT_P = "prob_emit.p"


# 状态转移矩阵，比如B状态前只可能是E或S状态
PrevStatus = {
    'B': 'ES',
    'M': 'MB',
    'S': 'SE',
    'E': 'BM'
}

Force_Split_Words = set([])
def load_model():
    start_p = pickle.load(get_module_res("finalseg", PROB_START_P))
    trans_p = pickle.load(get_module_res("finalseg", PROB_TRANS_P))
    emit_p = pickle.load(get_module_res("finalseg", PROB_EMIT_P))
    return start_p, trans_p, emit_p

if sys.platform.startswith("java"):

    start_P, trans_P, emit_P = load_model()
else:
    from .prob_start import P as start_P
    from .prob_trans import P as trans_P
    from .prob_emit import P as emit_P


def viterbi(obs, states, start_p, trans_p, emit_p):
    """
    :param obs: 观测序列 obs = sentence
    :param states: 隐藏状态集合 states = 'BMES'
    :param start_p: 初始状态概率向量
    :param trans_p: 状态转移矩阵
    :param emit_p: 观测概率矩阵(发射矩阵)
    :return:
    """
    V = [{}]  # tabular,字典列表
    path = {}
    # 初始化概率
    for y in states:  # init 初始化，初始概率*观测概率 states = 'BMES'
        # V[0][y]为V列表第一个元素:一个key为y的字典，y为'BMES'，V列表只有一个元素，有4个Key的字典
        V[0][y] = start_p[y] + emit_p[y].get(obs[0], MIN_FLOAT) # 初始概率*观测概率，已经求过np.log了，乘法变为加法，类似于这种[{'B': 0.8, 'M': 0.7, 'S': 0.9}]
        # path为4个key的字典，path['B'] = ['B'], path['E'] = ['E'], path['M'] = ['M'], path['S'] = ['S']
        path[y] = [y]

    # 递推步
    for t in xrange(1, len(obs)):
        # 给V添加一个字典，这个句子有长度多少就添加几个字典
        V.append({})
        newpath = {}

        # 真正循环句子计算最优路径
        for y in states: # states = 'BMES'
            # y状态下，观测到第t个字的概率，b(o)，即当前字观测概率，即发射概率
            em_p = emit_p[y].get(obs[t], MIN_FLOAT)
            # 转移概率，乘法转加法，V[t-1][y0]前一个字，状态为y0的概率，trans_p[y0].get(y)：y0状态转移到y状态的概率，
            # 求出了由y0的四个状态转到当前y状态的最大概率，及当时y0的状态，动态规划
            (prob, state) = max(
                [(V[t - 1][y0] + trans_p[y0].get(y, MIN_FLOAT) + em_p, y0) for y0 in PrevStatus[y]])
            # 记录下y0的四个状态转到每个y状态的最大概率
            V[t][y] = prob
            # 记录下前面最大概率路径和y的状态，并存到字典newpath[y]中，newpath[y]有4个key
            newpath[y] = path[state] + [y]
        # 更新path，path还是为4个key的字典，value为当前key状态的最大概率路径
        path = newpath

    # 终止步，最后一个元素，状态为E或S，取概率大的那个
    (prob, state) = max((V[len(obs) - 1][y], y) for y in 'ES')

    return (prob, path[state])


def __cut(sentence):
    global emit_P
    prob, pos_list = viterbi(sentence, 'BMES', start_P, trans_P, emit_P)
    begin, nexti = 0, 0
    # print pos_list, sentence
    for i, char in enumerate(sentence):
        pos = pos_list[i]
        if pos == 'B':
            begin = i
        elif pos == 'E':
            yield sentence[begin:i + 1]
            nexti = i + 1
        elif pos == 'S':
            yield char
            nexti = i + 1
    if nexti < len(sentence):
        yield sentence[nexti:]

re_han = re.compile("([\u4E00-\u9FD5]+)")
re_skip = re.compile("([a-zA-Z0-9]+(?:\.\d+)?%?)")


def add_force_split(word):
    global Force_Split_Words
    Force_Split_Words.add(word)

def cut(sentence):
    sentence = strdecode(sentence)
    blocks = re_han.split(sentence)
    for blk in blocks:
        if re_han.match(blk):
            for word in __cut(blk):
                if word not in Force_Split_Words:
                    yield word
                else:
                    for c in word:
                        yield c
        else:
            tmp = re_skip.split(blk)
            for x in tmp:
                if x:
                    yield x
