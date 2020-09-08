#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: rename.py
@time: 2019-07-12 16:48
@desc:用于重命名文件名字
'''
import os
import sys
import datetime


def is_in_dir(file_path, replace_name):
    for dir2 in dirs:
        if replace_name in dir2:
            print(dir2)


def rename_dir(file_path, replace_name):
    for (file_path, dirs, files) in os.walk(file_path):
        print(files)
        for dir_name in dirs:#替换文件夹名字
            if replace_name in dir_name:
                print(dir_name,dirs)
                print(dir_name.replace(replace_name,""))
                newname = dir_name.replace(replace_name,"")
                os.rename(file_path+"\\"+dir_name , file_path+"\\"+newname)


def rename_file(file_path, replace_name):
    for (file_path, dirs, files) in os.walk(file_path):
        print(files)
        for filename in files: #替换文件名字
            if replace_name in filename:
                print(filename,dirs)
                print(filename.replace(replace_name,""))
                newname = filename.replace(replace_name,"").replace(" ","")
                os.rename(file_path+"\\"+filename , file_path+"\\"+newname)


def remove_file(file_path):
    for (file_path, dirs, files) in os.walk(file_path):
        print(files)
        for filename in files: #替换文件名字
            if '.csv' in filename or '.pkl' in filename:
                day_str = (datetime.datetime.now()-datetime.timedelta(days=2)).strftime("%Y%m%d")
                if day_str in filename:
                    print(filename,dirs)
                    os.remove(file_path+filename)

def search_file(file_path, replace_name):
    """包含关键字www.17zixueba.com的文件名字，删选出过滤文件名"""
    keyword_list = set()
    for (file_path, dirs, files) in os.walk(file_path):
        for filename in files: #替换文件名字
            if replace_name in filename:
                keyword_list.add(filename)
    print(keyword_list)


if __name__ == '__main__':
    file_path = "F:/"
    dirs = os.listdir(file_path)
    name = "[www.17zixueba.com]"
    rename_dir(file_path, name)