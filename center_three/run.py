#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: run
@time: 2020/8/26 0026 13:44
@desc: 
'''


import sys
import os
import json
import pandas as pd
import threading

sys.path.append(os.environ['PUSHPATH'])
path =os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0,path)
from util.c_define import *
from util.mapDGG import *
from center_three.manager import Manager
import datetime
import time
from util.timer.sched import SchduleTimer
from center_three.view.getlabel.getlabel import *
manager = Manager()

label = GetLabel()
label.getregex()

def foo(message):

    print_to_log('message:', message)
    pid = message[PhonePID]
    # # TODO:1、从内存中获取历史数据
    # if pid in manager.session_dict.keys():
    #     history_session = manager.session_dict
    # else:
    #     history_session = list()
    #     manager.session_dict[pid] = history_session
    #
    audio_message = message[PhoneText]
    start = time.time()
    try:
        # TODO: 2、王辰提取标签
        W_result = label.match_label(message)
        print_to_log(audio_message)
    except Exception as e:
        print_to_log("'标签提取异常 %s %s' % (str(e), value)", e, level=4)
    try:
        # TODO: 3、罗成话术分支查询
        print_to_log(audio_message)
    except Exception as e:
        print_to_log("'话术分支查找异常 %s %s' % (str(e), value)", e, level=4)

    # TODO:4、将当前处理完的信息记录到内存中
    # history_session.append(audio_message)

    # TODO:5、将处理结果返回给张超
    print_to_log('总耗时:%.3f' % (time.time() - start))
    return None


def cycle_train():
    """
        空闲时间更新当天kafka的吞吐量
    """
    try:
        manager.update_data()
    except:
        print_to_log("Error: update_data", level=5)


def scheduled_task():
    sc = SchduleTimer(hour='23:59')
    sc.start(fun=cycle_train, cycle=55)


if __name__ == '__main__':
    """
    git fetch --all
    git reset --hard origin/dev
    git pull 
    """

    param = dict()
    param[PhoneText] = "你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗"
    param[PhonePID] = "1"
    param[PhoneUniqueID] = "1"
    param[PhoneYTCode] = ""
    param[PhoneDecibel] = 65
    param[PhoneSpeechSpeed] = 25
    param[PhoneCallDuration] = 2222
    param[PhoneState] = "start"
    param[PhoneCreateTime] = "2020-02-02 02:02:02"
    param[PhoneSessionRole] = "dggsb"
    param[PhoneSceneID] = "123"

    hehe = dict()
    hehe[PhoneText] = "嗯,你打错了吧"
    hehe[PhonePID] = "1"
    hehe[PhoneUniqueID] = "0"
    hehe[PhoneYTCode] = ""
    hehe[PhoneDecibel] = 65
    hehe[PhoneSpeechSpeed] = 25
    hehe[PhoneCallDuration] = 2222
    hehe[PhoneState] = "0"
    hehe[PhoneCreateTime] = "2020-02-02 02:02:02"
    hehe[PhoneSessionRole] = "dggsb"
    hehe[PhoneSceneID] = "123"

    pei = dict()
    pei[PhoneText] = "就是价格问题上回有一个给报报下来了这100多有点贵啊"
    pei[PhonePID] = "1"
    pei[PhoneUniqueID] = "0"
    pei[PhoneYTCode] = ""
    pei[PhoneDecibel] = 65
    pei[PhoneSpeechSpeed] = 25
    pei[PhoneCallDuration] = 2222
    pei[PhoneState] = "0"
    pei[PhoneCreateTime] = "2020-02-02 02:02:02"
    pei[PhoneSessionRole] = "dggsb"
    pei[PhoneSceneID] = "123"

    cao = dict()
    cao[PhoneText] = "你妹的瓜锤"
    cao[PhonePID] = "1"
    cao[PhoneUniqueID] = "0"
    cao[PhoneYTCode] = ""
    cao[PhoneDecibel] = 65
    cao[PhoneSpeechSpeed] = 25
    cao[PhoneCallDuration] = 2222
    cao[PhoneState] = "0"
    cao[PhoneCreateTime] = "2020-02-02 02:02:02"
    cao[PhoneSessionRole] = "dggsb"
    cao[PhoneSceneID] = "123"

    foo(param)
    foo(hehe)
    foo(pei)
    foo(cao)


    thread = threading.Thread(target=scheduled_task)
    # thread.start()
    # param = dict()
    #
    # param[PhoneText] = "text"
    # param[PhonePID] = "002"
    # param[PhoneUniqueID] = "123456"
    # param[PhoneYTCode] = "sbdgg"
    # param[PhoneDecibel] = "65"
    # param[PhoneSpeechSpeed] = "25"
    # param[PhoneCallDuration] = "39"
    # param[PhoneState] = "0"
    # param[PhoneCreateTime] = "2020-02-02 02:02:02"
    # param[PhoneSessionRole] = "dggsb"
    # param[PhoneSceneID] = "123"
    # foo(param)
