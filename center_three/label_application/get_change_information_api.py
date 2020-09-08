#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: get_change_information_api.py
@time: 2020/8/26 9:33 上午
@desc:
'''
from flask import Flask, Response, request
import flask
import time
import pandas as pd
import json

from center_three.label_application.parsing import get_df

## TODO: flask 接口
class IbaResponse(Response):
    default_mimetype = 'application/json'

class IbaFlask(Flask):
    response_class = IbaResponse

app = IbaFlask(__name__)

@app.route('/get_information/get_change', methods=['POST', 'GET'])
def get_information(param=None):

    # 1. 获取传入参数
    print(param)
    start = time.time()
    json_data = request.get_data()

    print("flask的get_data结果：%s" % json_data)
    print(json_data)
    # json_data = eval(json_data)
    # json_data = str(json_data,encoding="utf-8")
    json_data = json_data.decode(encoding="utf-8", errors="strict")  # bytes ----> str
    print("参数")
    json_data_list = eval(json_data)  # dict of list [{"label_id":1,"operate":"update"},{"label_id":2,"operate":"add"},{"label_id":3,"operate":"delete"}]
    print(json_data_list)

    # 2. 根据更改的数据进行数据的 重新读取 或 更新 或 删除
    ## 2.1 模拟数据库读取结果
    # test_dataframe = pd.DataFrame([[1, 2, 3], ["商务辱骂", "商务语速过快", "商务声音太大"]]).T
    # test_dataframe.columns = ["label_id", "label_name"]
    # test_dataframe.set_index("label_id",inplace=True)

    test_dataframe = get_df.rule_df
    global test_dataframe

    print("属性")
    print(test_dataframe)

    result_list = []
    ## 2.2 进行df的操作
    # 删除行
    for dic in json_data_list:
        operate = dic["operate"]
        id = dic["label_id"]

        # 删除某行
        if operate == "drop":
            test_dataframe = test_dataframe.drop([id], axis=0)
            get_df.rule_df = test_dataframe

            print("更改后的类属性")
            print(get_df.rule_df)

            dic["status"] = "success"
        elif operate == "add":
            pass

        elif operate == "update":
            pass

        else:
            print("操作动作失败，你得按我说的来")
            dic["status"] = "failed"
            continue

        result_list.append(dic)

    result_dict = {
        'code': '200',
        "msg": "请求成功",
        "data": result_list
    }
    print("结果")
    print(result_list)
    result = json.dumps(result_dict, ensure_ascii=False)
    # result = json.dumps(result_dict)
    print(result)
    print("返回预测请求........")
    return result

if __name__ == "__main__":
    #
    # 产线模式
    # app.run(debug=True, use_reloader=False,threaded=True)
    app.run(host='0.0.0.0', debug=True, port= 33333, use_reloader=False, threaded=True)



