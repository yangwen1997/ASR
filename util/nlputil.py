# coding=utf-8


'''
    使用NLTK和pynlpir(分词) 进行自然语言的处理，包括如下接口：
    1、语料中词汇出现频率
    2、词汇概率
    
'''

import nltk
import pynlpir as nlp
from nltk import *

def nlpText(sentence):
    nlp.open()

    tokens = nlp.segment(sentence, pos_english=False)
    tokens = [word[0] for word in tokens  if word[1] != "代词" \
              and word[1] != "标点符号" and word[1] != "连词" \
              and word[1] != "副词" and word[0] != "\\" ]
    text = nltk.Text(tokens)
    nlp.close()

    return text


s = "英国媒体报道，在即将启动的英国脱欧谈判中，欧盟方面准备向英国索要最多600亿英镑(约合5124亿元人民币)，被英国媒体调侃为\“史上最贵离婚赡养费\”。\
英国《金融时报》采访欧盟谈判团队的多名核心成员，初步估算出欧盟需要向英国索要的账单总额为400亿英镑(3416亿元人民币)至600亿英镑。\
这笔“离婚赡养费”涵盖的内容极广，例如英国先前承诺支付的欧盟预算份额、各种贷款担保、欧盟花费于英国境内的各种项目、英国先前承诺承担的欧盟员工和欧洲议会议员养老金份额等。\
英国《每日邮报》报道，欧盟的英国脱欧事务谈判团队将由前欧盟委员会内部市场与服务委员、法国人米歇尔·巴尼耶带队，预计将与英国方面就这份高昂账单展开一番“激烈谈判”。\
根据巴尼耶的时间表，英国政府3月底启动《里斯本条约》第50条；欧盟与英国最迟于2018年秋季拟定一份脱欧草案，2019年4月完成脱欧程序。\
巴尼耶谈判团队目前设想的是“硬脱欧”，即英国退出欧盟统一市场、今后能够单独达成各类贸易协定。不过，英国前副首相尼克·克莱格曾表示，他和其他“留欧”派议员将争取修改法律，力争实现\“软脱欧\”"

text = nlpText(s)

fdist = nltk.FreqDist(text)
# for key in fdist.keys():
#     if fdist[key] >1: print key,fdist[key]


# print StanfordTokenizer().tokenize(s)

import os
print(os.environ)