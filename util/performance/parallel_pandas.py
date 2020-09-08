#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: parallel_pandas.py
@time: 2019-05-07 13:51
@desc:并行执行pandas
'''

def parr_groupby(df):
    df = df.groupby(['login_name', 'business_id']) \
        .agg({'customer_id': lambda x: x.value_counts().index[0], 'content': np.sum, 'follow_days': np.mean,
              'follow_hour': np.mean}) \
        .reset_index()
    return df