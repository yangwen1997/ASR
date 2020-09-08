#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: run_time.py
@time: 2019-05-07 13:32
@desc:你想在脚本执行结束时测量一些基准数据，比如运行了多长时间
打眼看来很简单。只需要将代码添加到脚本的最底层，它将在脚本结束前运行。但如果脚本中有一个致命错误或者脚本被用户终止，它可能就不运行了。
当你使用atexit.register()时，你的代码都将执行，不论脚本因为什么原因停止运行。
'''
import atexit
import time
import math


def microtime(get_as_float = False):
    if get_as_float:
        return time.time()
    else:
        return '%f %d' % math.modf(time.time())

def shutdown():
    global start_time
    print("Execution took:{0} seconds".format(start_time))


if __name__ == '__main__':
    start_time = microtime(False)
    atexit.register(start_time)

    atexit.register(shutdown())