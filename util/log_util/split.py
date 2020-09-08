#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: split.py
@time: 2019-08-06 15:47
@desc:
'''

import re
import datetime
import os
import threading
from util.mapDGG import *


def split_by_day(days=None, log_path=None):
    """
    :param days: 如果list是多个，则每天一个文件, 如果len(days) == 1则会切分昨天到现在为止的日志
    :param log_path: online.txt所在的文件目录
    :return:
    """
    if os.path.exists(log_path):

        recent_index = 0
        dir_path = log_path.replace("online.txt", "")
        files = [open(dir_path + name + '.txt', 'w') for name in days]

        with open(log_path, 'rb') as log:
            lines = log.readlines()
            for line in lines:
                if isinstance(line, float) or isinstance(line, int) or line is None:
                    continue

                if isinstance(line, bytes):
                    line = bytes.decode(line)

                line_head = re.match(r"2019-\d{2}-\d{2}", line)
                # 判断是否需要更新日志文件
                if line_head is not None:
                    line_head = line_head.group()
                    if line_head in days and days.index(line_head) != recent_index:
                        recent_index = days.index(line_head)
                        print(recent_index)

                files[recent_index].write(line)

            for name in files:
                name.close()
    else:
        print(log_path, "不存在")


if __name__ == '__main__':
    # days = sorted([(datetime.datetime.now() - datetime.timedelta(i)).strftime('%Y-%m-%d') for i in range(30)],
    #               reverse=False)
    # split_by_day(days=days, log_path="/Users/luocheng/fsdownload/online.txt")

    days = ['20190802']
    thread = threading.Thread(target=split_by_day, args=(days, log_path + "online.txt"))
    thread.start()