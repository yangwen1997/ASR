#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: multiprocess_executor.py
@time: 2019-05-07 13:52
@desc:python多进程执行器
'''
import glob
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Executor
import cv2
import concurrent.futures
import time
from pandarallel import pandarallel


def load_and_resize(image_filename):
    img = cv2.imread(image_filename)
    img = cv2.resize(img,(600, 600))
    img = cv2.resize(img, (700, 700))
    img = cv2.resize(img, (1600, 1600))
    img = cv2.resize(img, (600, 600))


# 求最大公约数--计算密集型
def gcd(pair):
    a, b = pair
    low = min(a, b)
    for i in range(low, 0, -1):
        if a % i == 0 and b % i == 0:
            return i
import pandas as pd
import numpy as np

if __name__ == '__main__':
    # TODO: time python multiprocess_executor.py此命令查看脚本运行时间
    # image_files = glob.glob("*.png")
    # for image_name in image_files:
    #     load_and_resize(image_name)


    # 你有多少CPU核心就启动多少Python进程，在我的例子中是6个 有6个核心，我们将同时处理列表中的6个项
    with concurrent.futures.ProcessPoolExecutor() as executor:
        image_files = glob.glob("*.png")
        executor.map(load_and_resize, image_files) # executor.map（）将你想要运行的函数和一个列表作为输入，列表中的每个元素都是函数的单个输入。

    numbers = [
        (1963309, 2265973), (1879675, 2493670), (2030677, 3814172),
        (1551645, 2229620), (1988912, 4736670), (2198964, 7876293)
    ]

    # 不使用多线程、多进程
    start = time.time()
    results = list(map(gcd, numbers))
    end = time.time()
    print('Took %.3f seconds.' % (end - start))


    # 多线程 gcd是一个计算密集型函数，因为GIL的原因，多线程是无法提升效率的。同时，线程启动的时候，有一定的开销，与线程池进行通信，也会有开销，所以这个程序使用了多线程反而更慢了。
    start = time.time()
    pool = ThreadPoolExecutor(max_workers=3)
    results = list(pool.map(gcd, numbers)) # 也可以pool.submit submit和map的区别见https://www.cnblogs.com/huchong/p/7459324.html
    end = time.time()
    print('Took %.3f seconds.' % (end - start))

    # 多进程
    start = time.time()
    pool = ProcessPoolExecutor(max_workers=3)
    results = list(pool.map(gcd, numbers)) # 和内置函数map差不多的用法，这个方法返回一个map(func, *iterables)迭代器，迭代器中的回调执行返回的结果有序的。
    pool.shutdown()
    end = time.time()
    print('Took %.3f seconds.' % (end - start))

    df_size = int(5e6)
    df = pd.DataFrame(dict(a=np.random.randint(1, 8, df_size),
                           b=np.random.rand(df_size)))
    print(df.shape)

