#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: location_company
@time: 2020/8/4 0004 9:50
@desc: 
'''

import os
import sys

sys.path.append(os.environ['PUSHPATH'])
from model_center.named_entity_recognition.lstm_configuration import LSTMConfig
from model_center.named_entity_recognition.lstm_predict import LSTMNER


class SunDaLiConfig(LSTMConfig):
    cur = os.path.join(os.environ['PUSHPATH'], "center_three/data")
    data_path = cur
    model_path = cur

    train_path = os.path.join(data_path, 'train.txt')
    vocab_path = os.path.join(model_path, 'vocab.txt')
    embedding_file = os.path.join(model_path, 'token_vec_300.bin')
    model_path = os.path.join(model_path, 'tokenvec_bilstm2_crf_model_20.h5')

    class_dict = {'O': 0,
                  'I-TIME': 1,
                  'I-PER': 2,
                  'I-ORG': 3,
                  'I-LOC': 4,
                  'E-TIME': 5,
                  'E-PER': 6,
                  'E-ORG': 7,
                  'E-LOC': 8,
                  'B-TIME': 9,
                  'B-PER': 10,
                  'B-ORG': 11,
                  'B-LOC': 12}
    # 当前模型绑定的槽位字段 ['字段名','字段名']
    slot_list = []


model = LSTMNER(config=SunDaLiConfig)


def get_ner_result(line):
    ret = model.predict(line)
    name_list = list()
    name = ""
    for element in ret:

        if "O" in element:
            if len(name) > 0:
                name_list.append(name)
                name_list.append("\n")
            name = ""
        else:
            name = name + element[0]

    # 如果名字在最后面，不是以O结尾
    if len(name) > 0:
        name_list.append(name)

    print(name_list)
    value = " ".join(name_list)
    value = value.replace(" ", "")
    if len(value) > 0:
        return value
    else:
        return None


if __name__ == '__main__':
    while 1:
        line = input("输入:")
        print(get_ner_result(line))
