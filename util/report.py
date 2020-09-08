#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: report.py
@time: 2020/6/18 1:34 下午
@desc:
'''
import pandas as pd

# 1. 将metrics.classification_report转为dataframe
def classification_report2df(report):
    """将metrics.classification_report转为dataframe
    : params report,

    """
    # 1. 转list of arr
    report = report.splitlines()
    res = []
    # 2. 处理columns
    res.append([''] + report[0].split())

    # 3. 处理各类别内容
    for row in report[2:-4]:
        #         print(type(row))
        #         print(row)
        res.append(row.split())

    # 4. 处理accuracy,macro_avg,weighted_avg
    accuracy = report[-3].strip().split()
    accuracy_list = [accuracy[0]] + ["", ""] + accuracy[1:]
    res.append(accuracy_list)

    macro_avg = report[-2].strip().split()
    macro_avg_list = ["_".join(macro_avg[:2])] + macro_avg[2:]
    res.append(macro_avg_list)

    weighted_avg = report[-1].strip().split()
    weighted_avg_list = ["_".join(weighted_avg[:2])] + weighted_avg[2:]
    res.append(weighted_avg_list)

    # 3. 存csv
    df = pd.DataFrame(res)
    return df

