#!/usr/bin/env python
# encoding: utf-8
'''
@author: 杨文龙
@contact: yangwenlong@dgg.net
@file: baseClass.py
@time: 2020/9/3 16:36
@desc: 话术流程-主流程调用模块，
'''

import os
import sys
import json
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0,path)

from getlabel.getlabel import GetLabel
from label_application.get_reminder_context import GetReminderContent
from center_three.manager import Manager
from util.mapDGG import *
from center_three.util.utils import *
from util.c_define import *
manager = Manager()

class MainProcess(object):
    """
        主流程函数
    """
    def __init__(self):
        super().__init__()
        self.wc_label = GetLabel() #王辰的模块
        self.sxl_label = GetReminderContent()
        self.redis_pool = GetRedisClusterConn(RedisHosts, RedisPassWord)  # 创建redis_pool
        self.read = ReadFromMysql(mysql_host, mysql_user, mysql_password, mysql_port)  # 获取读取mysql连接池


    def getTag(self,parmas):
        """
            调用王辰的模块获取标签
        :param parmas: ASR返回的对话内容
        :return:
        """
        try:
            self.wc_label.getregex();result = self.wc_label.match_label(parmas);return result
        except Exception as e:
            print_to_log("'话术分支查找异常 %s %s' % (str(e), value)", e, level=4)

    def text_analysis(self,messages:dict) -> str:
        """
            接收语音识别后的文字进行逻辑处理
        :param messages : ASR传递过来的消息值
        :return:
        """
        try:

            assert messages,"未接收到识别的语音结果"
        except:

            return "未接收到识别的语音结果"
        else:

            # TODO: 1、接收ASR识别后的语音结果取出PID
            pid = messages[PhonePID];resut_tag=""

            try:
                # TODO:2、王辰标签提取
                resut_tag = self.getTag(messages)
                print(resut_tag)
            except Exception as e:
                print_to_log("'标签提取异常 %s %s' % (str(e), value)", e, level=4)

            try:
                # TODO:3、没有进行历史缓存有的话取出串联数据进行缓存
                redis_conn = self.redis_pool.connect(); redis_conn.rpush(f"label_history:{pid}",json.dumps(resut_tag))

                try:
                    redis_conn.delete(f"label_history:{pid}") if \
                        json.loads(redis_conn.lrange(f"label_history:{pid}",0,-1)[-1])["state"] == '0' else ""
                except:
                    pass
                else:
                    pass

            except Exception as e:
                print_to_log("'Redis缓存操作异常 %s %s' % (str(e), value)", e, level=4)

            # TODO:4、从缓存中获取规则

            # TODO:5、根据规则进行话术匹配

            # TODO:6、根据王辰的返回结果调用孙小龙的程序
            print(self.sxl_label.get_reminder_content(asr_dict = cao["asr_dict"]))
            return ""


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
asr_dict = {'pid': '1', 'unique_id': '0', 'LabelId': 11485,
            'QuestionOriginalText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
            'AnswerOriginalText': '嗯,你打错了吧', 'CallTime': '2020-02-02 02:02:02', 'CreateTime': '2020-09-03 16:16:05',
            'decibel': 65, 'speech_speed': 25,
            'talk_time': 2222, 'state': '0', 'QuestionLabel': '是否需要注销',
            'QuestionMatchText': '你好打扰你一下哈我是之前有联系过您的镗缸呃就您之前有在我们公司这儿是咨询了关于公司的一个注销业务请问您现在公司注销这块您办理好了吗',
            'AnswerLabel': '打错了', 'AnswerMatchText': '嗯,你打错了吧'}
cao["asr_dict"] = asr_dict

st = MainProcess()
st.text_analysis(messages=cao)