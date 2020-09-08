#!/usr/bin/env python
# encoding: utf-8
"""
@File    : 呵呵呵呵呵呵.py
@Time    : 2020/8/6 14:46
@Author  : W
@Software: PyCharm
"""
import time
import os
import sys
from importlib import reload
sys.path.append(os.environ['PUSHPATH'])
from datetime import datetime
from center_three.voice_label.remarks_quality import test

def table_data(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->2020-07-01 10:10:10
    """
    dt = datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
    return date

def main ():
    reload(test)
    TEST = test.my_test()
    getattr(TEST, "test1")()
    # del test


if __name__ == '__main__':
    while 1:
        main()
        print(table_data(time.time()))
        time.sleep(0.5)


