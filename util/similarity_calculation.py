#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: similarity_calculation.py
@time: 2019-04-30 14:15
@desc:
'''


def Euclidean_distance(p,q):
    """
    计算欧几里德距离：
    Args:
        p: 要计算向量p
        q: 要计算的向量q

    Returns:
        相似度分数
    """
    #如果两数据集数目不同，计算两者之间都对应有的数
    same = 0
    for i in p:
        if i in q:
            same +=1

    #计算欧几里德距离,并将其标准化
    e = sum([(p[i] - q[i])**2 for i in range(same)])
    return 1/(1+e**.5)



def pearson(p,q):
    """
    计算皮尔逊相关度：
    Args:
        p: 向量
        q: 向量

    Returns:
        相似度分数
    """
    #只计算两者共同有的
    same = 0
    for i in p:
        if i in q:
            same +=1

    n = same
    #分别求p，q的和
    sumx = sum([p[i] for i in range(n)])
    sumy = sum([q[i] for i in range(n)])
    #分别求出p，q的平方和
    sumxsq = sum([p[i]**2 for i in range(n)])
    sumysq = sum([q[i]**2 for i in range(n)])
    #求出p，q的乘积和
    sumxy = sum([p[i]*q[i] for i in range(n)])
    # print sumxy
    #求出pearson相关系数
    up = sumxy - sumx*sumy/n
    down = ((sumxsq - pow(sumxsq,2)/n)*(sumysq - pow(sumysq,2)/n))**.5
    #若down为零则不能计算，return 0
    if down == 0 :return 0
    r = up/down
    return r



def manhattan(p,q):
    """
    计算曼哈顿距离：
    Args:
        p: 向量
        q: 向量

    Returns:
        相似度分数
    """
    #只计算两者共同有的
    same = 0
    for i in p:
        if i in q:
            same += 1
    #计算曼哈顿距离
    n = same
    vals = range(n)
    distance = sum(abs(p[i] - q[i]) for i in vals)
    return distance


# 计算jaccard系数

#注意：在使用之前必须对两个数据集进行去重


if __name__ == '__main__':
    p = [1, 3, 2, 3, 4, 3]
    q = [1, 3, 4, 3, 2, 3, 4, 3]
    print(Euclidean_distance(p,q))

    print(pearson(p, q))

    print(manhattan(p, q))