#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: inspect_logging.py
@time: 2019-05-07 10:57
@desc:
'''
import inspect
import sys
import datetime
import traceback
from util.mapDGG import environment, log_path
import logging
"""
logging.basicConfig函数各参数: 

level总共分5个级别：debug < info< warning< error< critical 

日志信息低于设置的级别时，不予显示：如此处为最低级别debug，所以显示所以信息 
filename: 指定日志文件名 
filemode: 和file函数意义相同，指定日志文件的打开模式，’w’或’a’ 
format: 指定输出的格式和内容，format可以输出很多有用信息。显示的条目可以是以下内容： 
%(levelname)：日志级别的名字格式 
%(levelno)s：日志级别的数字表示 
%(name)s：日志名字 
%(funcName)s：函数名字 
%(asctime)：日志时间，可以使用datefmt去定义时间格式，如上图。 
%(pathname)：脚本的绝对路径 
%(filename)：脚本的名字 
%(module)：模块的名字 
%(thread)：thread id 
%(threadName)：线程的名字 
%(name)s	Logger的名字
%(levelno)s	打印日志级别的数值
%(levelname)s	打印日志级别名称
%(pathname)s	打印调用日志输出函数的模块的完整路径名，可能没有
%(filename)s	打印调用日志输出函数的模块的文件名
%(funcName)s	打印调用日志输出函数的函数名
%(module)s	打印调用日志输出函数的模块名
%(lineno)d	打印调用日志输出函数的语句所在的代码行号
%(created)f	当前时间，用UNIX标准的表示时间的浮 点数表示
%(relativeCreated)d	打印输出日志信息时的，自Logger创建以 来的毫秒数
%(asctime)s	字符串形式的当前时间。默认格式是 “2003-07-08 16:49:45,896”。逗号后面的是毫秒
%(thread)d	打印线程ID，可能没有
%(threadName)s	打印线程名，可能没有
%(process)d	打印进程ID
%(message)s	打印日志信息，即用户输出的消息
"""
# logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(filename)s:%(lineno)-4d: %(message)s', filename=data_path+'log.txt')
# # 获取被调用函数名称
#
# print(sys._getframe().f_code.co_name)
#
# # 获取被调用函数在被调用时所处代码行数
#
# print(sys._getframe().f_back.f_lineno)
#
# # 获取被调用函数所在模块文件名
#
# print(sys._getframe().f_code.co_filename)


levels = ['debug', 'info', 'warning', 'error', 'critical']


# *args,**kwargs
def print_to_log(*args, level=1):
    """
    :param args: 字符串，类似print函数，可以传入多个
    :param level: debug < info< warning< error< critical
    :return: 无返回值,直接写入log.txt文件
    """
    prefix = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if level > 3:
        frame, filename, line_number, function_name, lines, index = inspect.getouterframes(inspect.currentframe())[1]
        args = [prefix, levels[level - 1], str(filename).split('/')[-1],
                line_number, function_name, lines, index] + list(args)
        ex_type, ex_val, ex_stack = sys.exc_info()
        print(ex_type)
        print(ex_val)

        for stack in traceback.extract_tb(ex_stack):
            print(stack)
    else:
        args = [prefix] + list(args)

    args = [str(arg) for arg in args]
    if len(args) == 0:
        args = [prefix]
    string = '\t'.join(args)

    print(string)
    # if environment == 'RELEASE':
    #     print(string)
    #     return None

    # out_path = log_path + '{}_{}.txt'.format(prefix.split(" ")[0], levels[level - 1])  # .txt是不想被git跟踪
    #
    # logFile = logging.FileHandler(out_path, 'a')
    # # log格式
    # fmt = logging.Formatter(fmt='')
    # logFile.setFormatter(fmt)
    #
    # # 定义日志
    # if level == 1:
    #     logger1 = logging.Logger('logTest', level=logging.DEBUG)
    #     logger1.addHandler(logFile)
    #     logger1.debug(string)
    # elif level == 2:
    #     logger1 = logging.Logger('logTest', level=logging.INFO)
    #     logger1.addHandler(logFile)
    #     logger1.info(string)
    # elif level == 3:
    #     logger1 = logging.Logger('logTest', level=logging.WARNING)
    #     logger1.addHandler(logFile)
    #     logger1.warning(string)
    # elif level == 4:
    #     logger1 = logging.Logger('logTest', level=logging.ERROR)
    #     logger1.addHandler(logFile)
    #     logger1.error(string)
    # elif level == 5:
    #     logger1 = logging.Logger('logTest', level=logging.CRITICAL)
    #     logger1.addHandler(logFile)
    #     logger1.critical(string)
    # else:
    #     raise Exception


if __name__ == '__main__':
    print_to_log('我就想测试一下', level=2)
    print_to_log(level=3)

    print()
    try:
        100 / 0
    except Exception as e:
        print_to_log(e, level=4)

    print()
