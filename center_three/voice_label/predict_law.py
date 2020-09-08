#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: predict_law
@time: 2020/8/4 0004 11:19
@desc: 
'''


#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: config
@time: 2020/5/22 0022 10:37
@desc:
'''
import numpy as np
import pandas as pd
import sys
import os
import pickle
sys.path.append(os.environ['PUSHPATH'])

from model_center.text_classify.cnn.config import Config, query_path_with_local_file
from model_center.text_classify.cnn.cnn_model import TextCnn
from model_center.text_classify.cnn.data_processing import process_one_data, get_wordid, get_word2vec

import tensorflow as tf
import re
wordid = get_wordid(Config.vocab_filename)
Config.vocab_size = len(wordid)
Config.pre_trianing = get_word2vec(Config.vector_word_npz)
from util.c_define import *
from center_three.voice_label.train_law import reset_config


reset_config()
print("--------------------------")
print(Config.seq_length)
print("--------------------------")
os.environ["CUDA_VISIBLE_DEVICES"] = '-1'  # use GPU with ID=0
config = tf.ConfigProto()
config.gpu_options.per_process_gpu_memory_fraction = 0.1


try:
    tf.reset_default_graph()
    general_graph = tf.get_default_graph()
    general_model = TextCnn(config=Config, robot_id=None, learning_rate=Config.lr)
    general_sess = tf.Session(config=config)
    _, _, save_dir, _ = query_path_with_local_file()
    general_sess.run(tf.global_variables_initializer())

    save_path = tf.train.latest_checkpoint(save_dir)

    saver = tf.train.Saver()
    saver.restore(sess=general_sess, save_path=save_path)
    print("本地模型添加成功....")
except Exception as e:
    general_graph = None
    general_model = None
    general_sess = None


def predict(text=""):
    model = general_model
    temp_graph = general_graph
    temp_sess = general_sess

    x_pad, id_to_category = process_one_data(text, wordid, max_length=Config.seq_length, robot_id=None)

    with temp_graph.as_default():
        topk_label = temp_sess.run(model.top_index, feed_dict={model.input_x: x_pad,
                                                               model.keep_pro: 1.0})

        topk_probability = temp_sess.run(model.top_value, feed_dict={model.input_x: x_pad,
                                                                     model.keep_pro: 1.0}).tolist()
    print(topk_label)
    topk_label = [id_to_category[name] for name in topk_label[0]]
    return topk_label, topk_probability[0]


if __name__ == '__main__':
    # predict(text="国台办：坚决反对达赖赴台本报北京8月27日电(记者王尧) 今天，国务院台湾事务办公室发言人就台湾民进党部分势力邀请达赖访台一事表示：达赖不是单纯的宗教人士，他打着宗教的旗号，一直在进行分裂国家的活动。无论达赖以什么形式和身份赴台，我们都坚决反对。国台办发言人指出，正当大陆各界纷纷伸出援手，倾力支持台湾早日战胜风灾、重建家园之时，民进党的一些人竟趁机策划达赖到台活动，显然不是为了救灾，而是试图破坏两岸关系得来不易的良好局面，这一险恶用心必将遭到两岸同胞的共同反对。已有_COUNT_条评论我要评论大陆援台官员对台当局邀访达赖表示遗憾民进党邀达赖访台被指借救灾操弄政治台湾媒体称民进党邀访达赖含明显政治企图")
    # content = "我想注册商标"
    while (1):
        content = input('sentence:')
        print(predict(text=content))
