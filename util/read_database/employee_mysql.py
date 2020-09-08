#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: employee_mysql.py
@time: 2019-05-05 16:53
@desc:
'''


import pandas as pd
from base_class.read_mysql import Read_From_MySQL as readMysql


def read_employee_from_mysql():
    database = "db_iboss_allocat"
    columns = readMysql(type=3).column_from_mysql(database_name=database, table_name='bus_user_info')

    db = readMysql().getConnectionFromAllowAllotEmployee(database=database)
    cursor = db.cursor()
    sql = "select %s from bus_user_info where can_allot=1 and allow_allot_b > 0" % ",".join(columns)
    # 商机创建时间 要利用商机id，关联bus_business表
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        results_list = []
        for row in results:
            zipped = zip(columns, row)
            dic = {}
            for zips in zipped:
                dic[zips[0]] = zips[1]
            results_list.append(dic)

    except Exception as e:
        print("Error", e)
    finally:
        cursor.close()
        db.close()
        return results_list


if __name__ == '__main__':
    import sys
    import os
    from util.mapDGG import *
    sys.path.append(os.environ['PUSHPATH'])
    pd.set_option('display.width', 1000)
    # 显示所有列
    pd.set_option('display.max_columns', 1000)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    # 设置value的显示长度为100，默认为50
    pd.set_option('max_colwidth', 1000)
    employees = pd.DataFrame(read_employee_from_mysql())
    employees.to_csv("employees.csv", encoding='utf-8', sep=',', index=0)
    print(employees.shape)
    employees[residual_number] = employees[allow_allot_b] - employees[allotted_count_b]
    # login_names = employees[employees['id'] == 4116]['user_no'].values
    print(employees[employees['id'] == 4116])