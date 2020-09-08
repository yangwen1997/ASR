#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: search_file.py
@time: 2019-05-07 10:49
@desc:利用glob来查找文件
'''


import glob
import itertools as it,glob
import os


def mutiple_file_types(*patterns):
    return it.chain.from_iterable(glob.glob(pattern) for pattern in patterns)


if __name__ == '__main__':
    # 查找当前路径下的所有py文件
    files = glob.glob('*.py')
    print(files)

    for filename in mutiple_file_types("*.md", "*.py"):
        print(filename)
        real_path = os.path.realpath(filename)
        print(real_path)