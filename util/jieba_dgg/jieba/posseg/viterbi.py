#!/usr/bin/env python
# encoding: utf-8
'''
@author: 结巴源码 罗成 第一次修改
@contact: luocheng@dgg.net
@file: lgb_train.py
@time: 2019-07-10 15:49
@desc: 未登陆词的分词 状态是语料库中状态的入参，真正的Viterbi算法  不是finalseg\__init__.py这种简单的viterbi
'''

import sys
import operator
MIN_FLOAT = -3.14e100
MIN_INF = float("-inf")


# python 中获取版本号的方法
if sys.version_info[0] > 2:
    xrange = range


def get_top_states(t_state_v, K=4):
    # https://blog.csdn.net/chituozha5528/article/details/78354833
    return sorted(t_state_v, key=t_state_v.__getitem__, reverse=True)[:K]


def viterbi(obs, states, start_p, trans_p, emit_p):
    """
    :param obs: 观测序列 obs = sentencec
    :param states: 隐藏状态集合 states = 'BMES'
    :param start_p: 初始状态概率向量
    :param trans_p: 状态转移矩阵
    :param emit_p: 观测概率矩阵(发射矩阵)
    :return:
    """
    V = [{}]  # tabular 状态转移矩阵
    mem_path = [{}]
    all_states = trans_p.keys() # 所有的状态[('B', 'y'), ('E', 'h'), ('E', 'jn'), ('B', 'k'), ('B', 'qe'), ('E', 'vn'),
    # 初始化概率，只获取第一个字的初始概率
    for y in states.get(obs[0], all_states):  # init 获取输入字的第一个单词的状态
        # V[0][y]为V列表第一个元素:一个key为y的字典，y为'BMES'，V列表只有一个元素，有4个Key的字典
        # 初始概率*观测概率，已经求过np.log了，乘法变为加法，即{('B', 'y'): -3.14e+100}不断往后面叠加
        V[0][y] = start_p[y] + emit_p[y].get(obs[0], MIN_FLOAT)
        # {('B', 'y'): '', ('E', 'h'): '', ('E', 'jn'): '', ('B', 'k'): '', ('B', 'qe'): ''.........}
        mem_path[0][y] = ''
    # 递推步
    for t in xrange(1, len(obs)):
        V.append({})  # 给V添加一个字典，这个句子有长度多少就添加几个字典
        mem_path.append({})
        # prev_states = get_top_states(V[t-1])
        # prev_states  即是所有转移概率中存在的状态对应的值{('B', 'a'): {('E', 'a'): -0.0050648453069648755,('M', 'a'): -5.287963037107507},...} 中的key
        prev_states = [
            x for x in mem_path[t - 1].keys() if len(trans_p[x]) > 0]

        # prev_states_expect_next {('B', 'a'): {('E', 'a'): -0.0050648453069648755,('M', 'a'): -5.287963037107507},...} 中的key中key即('B', 'a')对应的('E', 'a')和('M', 'a')并去重
        prev_states_expect_next = set(
            (y for x in prev_states for y in trans_p[x].keys()))
        # 这里set取的是第一个和第二个参数的交集，其实放的是初始到下一个可能的状态
        obs_states = set(
            states.get(obs[t], all_states)) & prev_states_expect_next

        # 这里做一个非空的判断
        if not obs_states:
            obs_states = prev_states_expect_next if prev_states_expect_next else all_states

        # 真正循环句子计算最优路径
        for y in obs_states:
            # 转移概率，乘法转加法，V[t-1][y0]前一个字，状态为y0的概率，trans_p[y0].get(y)：y0状态转移到y状态的概率，
            # 求出了由y0的状态转到当前y状态的最大概率，及当时y0的状态，动态规划
            prob, state = max((V[t - 1][y0] + trans_p[y0].get(y, MIN_INF) +
                               emit_p[y].get(obs[t], MIN_FLOAT), y0) for y0 in prev_states)
            # 记录下y0的四个状态转到每个y状态的最大概率
            V[t][y] = prob
            # 记录下前面最大概率路径和y的状态，并存到字典mem_path[y]中，mem_path[y]有4个key
            mem_path[t][y] = state

    # 取出mem_path中第二个字典的所有的key开始循环，取出最后一个字状态的概率和状态
    last = [(V[-1][y], y) for y in mem_path[-1].keys()]
    # if len(last)==0:
    #     print obs
    prob, state = max(last)
    # 以汽车为例，route=[None, None, None, None, None, None]，python中把一个汉字是3个字长度
    route = [None] * len(obs)
    i = len(obs) - 1
    while i >= 0:
        route[i] = state
        # mem_path放的是y对应的y0的状态
        state = mem_path[i][state]
        i -= 1
    return (prob, route)
