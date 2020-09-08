# coding=utf-8
from dmp.store.db import get_db_instance
from util.log import logger
from pandas import DataFrame
import numpy as np
import pandas as pd

'''
    author@guomz
    function: 指标中需要计算统计数据，聚合函数等变化
'''


# sql形式的算子处理, 根据返回记录条数决定返回结果为DataFrame或单一数据
def pro_sql_method(sqlstr, param=None, db_id=None):
    result = get_db_instance(db_id=db_id).query(sqlstr, param)
    if result:
        return result if len(result) > 1 else result[0][0]
    return None


# 计算平均增长率
def calc_average_growth_rate(param):
    logger.debug("call the  calc_average_growth_rate method")
    param = DataFrame(param) if not isinstance(param, DataFrame) else param
    result = ((param[0] - param[0].shift(1)) / param[0].shift(1)).mean()
    return round(result, 3)


# 计算平均波动率
def calc_average_volatility(param):
    logger.debug("calc_average_volatility(list)")
    result = DataFrame(((param[0] - param[0].shift(1)) / param[0].shift(1)))
    return calc_average_growth_rate(result)


# 计算分类占比
def calc_class_ratio(param, type_flag):
    logger.debug("calc_class_ratio:{0}, flag:{1}".format(param, type_flag))
    x = DataFrame(param)
    result = round(float(x[0][x[0] == type_flag].count()) / x[0].count(), 3)
    return result

#返回数据的相关性
def get_corr(data1,data2):
    logger.debug('计算dataFrame数据的先关性')
    df1 = pd.DataFrame(data1,columns=['exp_date','p1'])
    df2 = pd.DataFrame(data2,columns=['exp_date','p2'])
    df = pd.concat([df1['p1'],df2['p2']],axis=1)
    corr = df.corr()
    return corr["p2"][0]

if __name__ == "__main__":
    # demo_data = DataFrame([1, 2, 4, 8, 16, 32, 64, 128])
    # print calc_average_growth_rate(demo_data)
    # print calc_average_volatility(demo_data)
    # sql = "select * from spider.company where title='中国联通' limit 2"
    # print pro_sql_method(sql)
    # print 'next ...'
    # sql = "select * from spider.company_1 where title=%(title)s limit 2"
    # print pro_sql_method(sql, {'title': '康师傅'})
    # print 'xxxx ...'
    # print eval("pro_sql_method(sql, {'company': '\u5eb7\u5e08\u5085'.decode('utf-8')})")
    print(pro_sql_method(
        "select advantage from spider.company_1 where title = %(company)s union all select defect from spider.company_1 where title = %(company)s ",
        {"company": "康师傅"}))
