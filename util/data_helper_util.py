#!coding=utf-8
import traceback
import pymongo
import logging
import sys
from sklearn.manifold import TSNE
import pdb
import arctic
import os
import gensim
import traceback
import numpy as np
import re
import jieba
import jieba.posseg
import const
Const = const._const()
Const.__setattr__("SUCC", "\n> success")
Const.__setattr__("FAIL", "\n> fail")
Const.__setattr__("ERROR", "\n> error")
Const.__setattr__("TEXTUSELESS", "\n无效原文 continue")
Const.__setattr__("TARGETUSELESS", "\n无效目标词 continue")
Const.__setattr__("KEYLOSS", "\n无该key continue")
Const.__setattr__("CLASSIFY_BATCH", "\n输出分类样本batch")
Const.__setattr__("DICT_LOST", "\n该词语在词典中并不存在")
Const.__setattr__("DEBUG",False)
Const.__setattr__("FLAG",False)
Const.__setattr__("NOUS",['n','s'])
Const.__setattr__("NS",['ns'])
Const.__setattr__("NZ",['nz'])
Const.__setattr__("NR",['nr'])
Const.__setattr__("PJ",['p'])
Const.__setattr__("AD",['ad','d','a'])
Const.__setattr__("V",['v','vn'])
Const.__setattr__("NUM",['m'])
Const.__setattr__("ENG",['eng'])
Const.__setattr__("X",['x'])

def abspath(filepath):
    """
    当前路径下的某个文件
    """
    CURPATH = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(CURPATH, filepath)

def format_str(*para):
    strout=""
    lenth = len(para)
    for i in range(lenth):
        strout+="{%s},"%i
    return strout[:-1].format(*para)

def clr_2_lst(sent):
    """
    过滤字符，输出list
    """
    res = []
    for i in list(jieba.posseg.cut(sent)):
        if not i.flag == "x":
            res.append(i.word)
    return res

def clr_2_str(sent):
    """
    过滤字符，输出res
    """
    res = ""
    for i in list(jieba.posseg.cut(sent)):
        if not i.flag == "x":
            res+=i.word
    return res

def nclr(sent):
    sent = re.sub("[^0-9]","", sent)
    return list(sent)

def cclr(sent):
    sent = re.sub("[^a-zA-Z]","", sent)
    return list(sent)

def mclr(sent):
    sent = re.sub("[^\u4e00-\u9fa5a-zA-Z0-9]","", sent)
    return list(sent)

def clr(sent):
    """
    过滤无用文本
    """
    sent = re.sub("[^\u4e00-\u9fa5a-zA-Z0-9]", "", sent)
    return sent

def random_lst(ll):
    np.random.shuffle(ll)
    return ll

def throw_exception(sent):
    raise Exception(sent)

def _vali_type(dat,tp,name):
    try:
        assert type(dat) == tp
        return Const.SUCC
    except AssertionError:
        sent=format_str('\n>In function',name,'\n> the type of',dat,'!=equal',tp)
        throw_exception(sent)
        return Const.ERROR

def _vali_equal(left,right,relation,name):
    try:
        if relation=="==":
            assert left==right
        elif relation==">":
            assert left>right
        elif relation=="<":
            assert left<right
            return Const.SUCC
    except AssertionError:
        sent=format_str('\n>In function',name,'\n> %s and %s is not %s '%(left,right,relation))
        throw_exception(sent)
        return Const.ERROR

def _vali_in(child,parent,name):
    try:
        if type(parent)==dict:
            assert (child in parent) ==True
        elif type(parent)==list:
            assert (child in parent) ==True
        elif type(parent)==tuple:
            assert (child in parent) ==True
        elif type(parent)==set:
            assert (child in parent) ==True
            return Const.SUCC
    except AssertionError:
        sent=format_str('\n>In function',name,'\n> %s is not in %s '%(child, parent))
        throw_exception(sent)
        return Const.ERROR

def _vali_date_lenth(dat,lenth,name):
    try:
        assert type(dat) == list or tuple
    except AssertionError:
        sent = format_str('\n>In function',name,'\n> the type of', dat,' has no function len(), only list and tuple has lenth')
        throw_exception(sent)
        return Const.ERROR
    try:
        assert len(dat) == lenth
        return Const.SUCC
    except AssertionError:
        sent = format_str('\n>In function',name,'\n> the lenth of', dat,'!=equal',lenth)
        throw_exception(sent)
        return Const.ERROR

def toLst(s):
    if type(s)==list:
        return s
    elif type(s)==str:
        return s.split(",")
    elif type(s)==tuple:
        return list(s)

def toStr(s):
    if type(s)==list:
        return ",".join(s)
    elif type(s)==str:
        return s
    elif type(s)==tuple:
        return ",".join(list(s))

def cutSent2WordsId(sent, dct):
    ids = []
    words = list(jieba.cut(sent))
    for word in words:
        try:
            _id = dct.token2id[word]
            ids.append(_id)
        except KeyError:
            _id = Const.DICT_LOST
        if _id == Const.DICT_LOST:
            continue
    return ids

def addDocs2Dct(words, dct, dctpath):
    """
    des:
        将新的words加入dictionary
    in:
        2d lst
    """
    dct.add_documents(words)
    dct.save(dctpath)

def concatWords(line, timeslamp_size):
    """
    in:
        line: 输出文本 str
        timeslamp_size: 输出长度 int 应当大于len(line)
    des:
        将一行文本，分词后按照词性重新排列，并按照timeslamp_size循环补齐输出
    ex:
        concatWords("北京天安门", 10)
    out:
        ["北京","天安门","北","京","天","安","门","北京","天安门","北"]
    """
    res = []
    nr,nz,ns,n,p,a,v,e,nu,x,tt,tt2 = [],[],[],[],[],[],[],[],[],[],[],[]
    words = list(jieba.posseg.cut(clr(line)))
    for word in words:
        if word.flag in Const.NS:
            ns.append(word.word)
        if word.flag in Const.NR:
            nr.append(word.word)
        if word.flag in Const.NZ:
            nz.append(word.word)
        if word.flag in Const.NOUS:
            n.append(word.word)
        elif word.flag in Const.PJ:
            p.append(word.word)
        elif word.flag in Const.AD:
            a.append(word.word)
        elif word.flag in Const.V:
            v.append(word.word)
        elif word.flag in Const.ENG:
            e.append(word.word)
        elif word.flag in Const.NUM:
            nu.append(word.word)
        elif word.flag in Const.X:
            x.append(word.word)
        if not word.flag in Const.X:
            tt.append(word.word)
    tt2.append(word.flag)
    tt.extend(tt2)
    tt.extend(e)
    tt.extend(x)
    tt.extend(nu)
    tt.extend(v)
    tt.extend(a)
    tt.extend(p)
    tt.extend(n)
    tt.extend(list(line)) #将字向量引入
    res.extend(tt)
    while(1):
        res.extend(res)
        if len(res)>timeslamp_size:
            break
    return res[:timeslamp_size]

def textLst2batch(txtLst, batch_size, timeslamp_size, dct):
    """
    des:
        将输入的二维数组文本，转化为batch输出
    ex:
        None
    in:
        txtLst: 输入文本 2d_lst
        batch_size: 每个batch里面数量
    out:
        二维数组 2d list
        [["北京","天安门","北","京","天","安","门","北京","天安门","北"],
        ["北京","天安门","北","京","天","安","门","北京","天安门","北"],
        ["北京","天安门","北","京","天","安","门","北京","天安门","北"],
        ["北京","天安门","北","京","天","安","门","北京","天安门","北"]]
    """
    resLst = []
    for i in range(batch_size):
        res = []
        if i < len(txtLst):
            wordLst = concatWords(txtLst[i], timeslamp_size)
            for word in wordLst:
                res.append(dct.token2id[word])
        else:
            wordLst = concatWords(txtLst[-1], timeslamp_size)
            for word in wordLst:
                res.append(dct.token2id[word])
        resLst.append(res)
    return resLst

def genDct(dctPath, sents):
    """
    in:
        输入一维数组
    out:
        生成词典,并保存
    """
    #dirpath = "/home/distdev/src/iba/dmp/gongan/labelmarker/data"
    #filename = "train.txt.bak"
    #res , reslb = read_file_2_possge(dirpath,filename,1)
    #res = [i[-1] for i in res]
    words = []
    for sent in sents:
        wordflags = list(jieba.posseg.cut(sent))
        words.append([i.word for i in wordflags])
        words.append([i.flag for i in wordflags])
        words.append(list(sent))
    dct = gensim.corpora.dictionary.Dictionary(words)
    dct.save(dctPath)

def loadDct(dctPath):
    """
    in:
        输入dictionary 路径
    out:
        返回dictionary
    """
    #dirpath = "/home/distdev/src/iba/dmp/gongan/labelmarker/data"
    #filename = "train.txt.bak"
    #res , reslb = read_file_2_possge(dirpath,filename,1)
    #res = [i[-1] for i in res]
    dictionary = gensim.corpora.dictionary.Dictionary.load(dctPath)
    return dictionary

def concatKeyWordsTfIdf(dictionary="./dictionary",corpus_path="./corpus.mm",tfidf_path="./tfidf.model",res=""):
    """
    in:
        words 二维数组
        dct 词典
        corpus 向量保存位置
        tfidf_path tfidf模型保存位置
    out:
        输出words,将原有文本与keywords,按行绑定输出
    """
    corpus = [dictionary.doc2bow(text) for text in res]
    gensim.corpora.MmCorpus.serialize(corpus_path, corpus) # 保存稀疏向量
    tfidf_model = gensim.models.TfidfModel(corpus)
    corpus_tfidf = tfidf_model[corpus]
    tfidf_model.save(tfidf_path)
    docs = []
    tfidfsum = 0
    cnt = 0
    for sent in corpus_tfidf:
        for word in sent:
            tfidfsum+=word[1]
            cnt+=1
    avr = tfidfsum/cnt
    for sent in corpus_tfidf:
        keywords = []
        for word in sent:
            if word[1]>avr:
                keywords.append(dictionary.get(word[0]))
        docs.append(keywords)
    return [sent.extend(keywords) for sent,keywords in zip(docs,res)]

if __name__ == "__main__":
    pass
    words = ["和谈顺利进行","坂门店局势紧张","北京天气晴朗","华盛顿一片乌云"]
    genDct("./dictionary", words)

    """
    dct 生成词典
    """
    dct = loadDct("dictionary")
    print([cutSent2WordsId(word,dct) for word in words])
    print("==="*3)

    """
    itextLst2batch 生成batch 输出
    """
    _ = textLst2batch(words, 32, 32, dct) #形成[32,32]矩阵
    print(len(_))
    print(len(_[0]))

