#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: sched.py
@time: 2019-07-09 14:27
@desc: 其他的定时器：https://www.jianshu.com/p/403bcb57e5c2
'''


from sched import scheduler
import time
from datetime import datetime
import os
import sys

sys.path.append(os.environ['PUSHPATH'])


class SchduleTimer(object):
    def __init__(self, hour='04:00'):
        # 初始化sched模块的scheduler类
        # 第一个参数是一个可以返回时间戳的函数，第二参数可以在定时未到达之前阻塞
        self.schdule = scheduler(time.time, time.sleep)
        self.event = None
        self.hour = hour

    # 被周期性调度触发函数
    def cycle_func(self, inc, func):
        now = datetime.now().strftime("%H:%M")
        if now == self.hour:
            func()
            self.schdule.enter(60 * 60 * 23, 0, self.cycle_func, (inc, func))
        else:
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'sleep until %s...' % self.hour)
            # enter四个参数分别为：间隔时间,优先级（用于同时到达两个事件同时执行的顺序），被调度触发的函数 给该触发器函数的参数（tuple形式）
            self.schdule.enter(inc, 0, self.cycle_func, (inc, func))

    # 默认参数60s
    def start(self, fun=None, cycle=60):
        self.schdule.enter(0, 0, self.cycle_func, (cycle, fun))
        self.schdule.run()

    # 被周期性调度触发的函数
    def cycle_func_by_second(self, cycle_time=60, func=None, *args):
        print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        need_stop = func(args[0])
        if need_stop is not None and need_stop:
            print("任务结束")
        else:
            self.event = self.schdule.enter(cycle_time, 0, self.cycle_func_by_second, (cycle_time, func, *args))

    # 默认参数60s *args: fun=None, cycle_time=60, rotbot_id=0
    def start_by_second(self, *args):
        # enter四个参数分别为：间隔事件、优先级（用于同时间到达的两个事件同时执行时定序）、被调用触发的函数，
        # 给该触发函数的参数（tuple形式）
        new_args = list()
        if len(args) > 2:
            for i in range(2, len(args)):
                name = args[i]
                new_args.append(name)
        print(new_args)
        self.schdule.enter(0, 0, self.cycle_func_by_second, (args[1], args[0], *new_args))
        self.schdule.run()

    # 取消任务
    def cancel_func(self):
        print(self.schdule.empty())
        self.schdule.cancel(event=self.event) # 不能取消 要报错


def test(robot):
    print("我被测试了", robot)
    return True


if __name__ == '__main__':
    sc = SchduleTimer(hour='16:00')
    sc.start_by_second(test, 5, 10)
