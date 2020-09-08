#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: train_law
@time: 2020/8/4 0004 10:43
@desc: 
'''
import sys
import os
sys.path.append(os.environ['PUSHPATH'])
from model_center.text_classify.cnn.config import Config
from model_center.text_classify.cnn.train import train


def reset_config():

    Config.seq_length = 100  # max length of sentence
    Config.num_epochs = 80
    Config.data_path = os.environ['PUSHPATH'] + '/center_three/data/'

    Config.labels_path = Config.data_path + "labels/"
    Config.contents_path = Config.data_path + "contents/"
    Config.model_path = Config.data_path
    Config.tensorboard_path = Config.data_path

    Config.train_filename = Config.data_path + 'train.txt'  # train data
    Config.test_filename = Config.data_path + 'test.txt'  # test data
    Config.val_filename = Config.data_path + 'valid.txt'  # validation data
    Config.stopwords_path = Config.data_path + "stopwords.txt"  # 停用词表

    # 设置字向量或词向量的文件路径
    # if is_char_vector:
    #     vocab_filename = data_path + 'char_vocab.txt'  # vocabulary
    #     # vector_word_filename = data_path + 'vector_word.txt'  #vector_word trained by word2vec
    #     vector_word_npz = data_path + 'char_vector.npz'  # save vector_word to numpy file
    #
    # else:
    #     vocab_filename = data_path + 'small_Tencent_dictionary.txt'  # vocabulary
    #     # vector_word_filename = data_path + 'vector_word.txt'  #vector_word trained by word2vec
    #     vector_word_npz = data_path + 'Tencent_vector_word_7W+.npz'  # save vector_word to numpy file


def train_law():
    reset_config()
    train()
    print("--------------------------")
    print(Config.seq_length)
    print("--------------------------")


if __name__ == '__main__':
    train_law()
