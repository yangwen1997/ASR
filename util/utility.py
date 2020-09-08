# coding=utf-8
import configparser
import os
import chardet
import datetime
import json
import uuid
import zipfile
from pandas import DataFrame as df
import math
import matplotlib.pyplot as plt
from util.pycrypt import prpcrypt


# python datetime.datetime is not JSON serializable 报错问题解决
class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat().replace('T', ' ')
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        else:
            return json.JSONEncoder.default(self, obj)


def get_config(cfgfile=None):
    if cfgfile is None:
        cfgfile = os.environ['DMPPATH'] + '/conf/globalsetting.cfg'
    config = configparser.ConfigParser()
    cf = open(cfgfile, 'r')
    config.read(cfgfile,encoding='utf-8')
    return config


def get_charset(char):
    try:
        return chardet.detect(char)['encoding']
    except Exception as ex:
        return 'utf-8'


# 计算时间差，单位为天
def get_day_diff(d1, d2):
    d1_y, d1_m, d1_d = d1.split("-")
    d2_y, d2_m, d2_d = d2.split("-")
    return abs((datetime.datetime(int(d1_y), int(d1_m), int(d1_d)) - datetime.datetime(
        int(d2_y), int(d2_m), int(d2_d))).days)


def decode_value_range(value_range):
    rules = []
    if value_range:
        if value_range == '*':
            rules.append('*')
        else:
            ranges = value_range.split('|')
            for r in ranges:
                values = r.split(':')
                if len(values) == 1:
                    try:
                        field = int(values[0]) if type(eval(values[0])) == int else float(values[0])
                    except Exception as ex:
                        field = values[0]
                else:
                    v0 = int(values[0]) if type(eval(values[0])) == int else float(values[0])
                    v1 = int(values[1]) if type(eval(values[1])) == int else float(values[1])
                    field = (v0, v1)
                rules.append(field)
    else:
        rules.append('')
    return rules


def match_index_rule(rules, persondata):
    default_score = None
    if not rules:
        raise ValueError

    if '*' in rules[0][1]:
        return persondata

    for rule in rules:
        credit_id = rule[0]
        if rule[1][0] == '' or rule[1][0] is None:
            default_score = rule[0]
        for rule_index in rule[1]:
            if isinstance(rule_index, tuple):
                if rule_index[0] <= persondata < rule_index[1]:
                    # log.debug(persondata, rule_index)
                    return credit_id
            else:
                if persondata == rule_index:
                    # log.debug(persondata, rule_index)
                    return credit_id
    if default_score is not None:
        return default_score
    else:
        raise ValueError


# 法人 'L' 自然人 'N' 家庭 'F' 系统 'S'
def generate_short_uuid(user_type):
    chars = ["a", "b", "c", "d", "e", "f",
             "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s",
             "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5",
             "6", "7", "8", "9", "A", "B", "C", "D", "E", "F", "G", "H", "I",
             "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V",
             "W", "X", "Y", "Z"]

    longid = ''.join(uuid.uuid4().__str__().split('-'))
    shortid = ''
    for x in range(0, 8):
        s = longid[x * 4:x * 4 + 4]
        x = int(s, 16)
        shortid += chars[x % 0x3E]

    return user_type + shortid


def unzip_file(path=None):
    if path:
        if os.path.exists(path):
            os.chdir(path)
        else:
            return False
    for filename in os.popen("find ./ -type f|grep -E '.zip$'"):
        filename = filename.rstrip('\n')
        with zipfile.ZipFile(filename) as zfobj:
            zfobj.extractall()
    return True


# 字符串list比较，获取最大匹配结果内容其中马头车的list为被匹配的字符串列表。
def max_match_list(reglist, matchedlist, split_char):
    score = []
    for item in matchedlist:
        score.append(len(set(reglist) & set(matchedlist)) / item.count(split_char) * 1.00)
    max_s = max(score)
    loc = score.index(max_s)
    print((matchedlist[loc]))


# 字符串list比较，获取最大匹配结果内容其中马头车的list为被匹配的字符串列表。
def max_match_list(reglist, matchedlist, split_char):
    score = []
    for item in matchedlist:
        a = len(set(reglist) & set(matchedlist))

        b = len((item[0].split(split_char))) * 1.00
        score.append(a / b)
    max_s = max(score)
    loc = score.index(max_s)
    return matchedlist[loc]


# 求正态分布的相关期望值和方差，参数为价格列表（list）
def normaldata(prices):
    change = [((prices[i + 1] - prices[i]) / prices[i] * 100.00) for i in range(0, len(prices) - 1)]
    change.insert(0, 0.000)
    # 查看图形是否符合正态分布
    normalData = {}
    for i in range(-800, 1000, 1):
        for item in change:
            if item > float(i / 10) and item < float((i + 1) / 10):
                if float(i * 1.0 / 10) in normalData:
                    normalData[float(i * 1.0 / 10)] = normalData[float(i * 1.0 / 10)] + 1
                else:
                    normalData[float(i * 1.0 / 10)] = 1

    plt.plot(sorted(normalData), [normalData[key] for key in sorted(normalData)], 'r--')
    plt.bar(sorted(normalData), [normalData[key] for key in sorted(normalData)], alpha=.5, color='b')
    # plt.show()

    mul = sum(change) / len(change)
    df_data = df(data=change, columns=['data'])
    df_data = df_data[df_data['data'] < 100]  # 排除奇点，例如涨跌幅超过100%的情况。
    df_data["mul"] = mul
    df_data["std"] = df_data["data"] - df_data["mul"]
    df_data["std1"] = df_data["std"] * df_data["std"]
    std = math.sqrt(df_data["std1"].sum() / len(df_data))

    return mul, std


def decrypt_sentences(sentences, encrypt=False):
    """
    text解密
    :param sentences:
    :param encrypt:
    :return:
    """
    encrypt = True if (isinstance(encrypt, bool) and encrypt) or (isinstance(encrypt, str) and encrypt.upper() == 'TRUE') else False
    if encrypt:
        pc = prpcrypt(get_config().get('encryptKey', 'key'))
        if isinstance(sentences, list):
            return [pc.decrypt(bytes(item, encoding='utf-8')) for item in sentences]
        elif isinstance(sentences, str):
            return pc.decrypt(bytes(sentences, encoding='utf-8'))
        return None
    else:
        return sentences


if __name__ == "__main__":
    result = unzip_file('./test/data')
    #f = open('D:\data\uuid.txt', 'w')
    #for i in range(0, 100):
        # print generate_short_uuid('')
     #   f.write(generate_short_uuid('') + '\n')

    #f.close()

    cfg = get_config()
    print(cfg.get('mysql', 'password'))

    sss = 'ce01fc5bbb7c8671a7d91da7a369ac1f3893b27f9aa69c1b66ed6a3f17f44a1515aec627581af64ae0d27b00451da6d6211a249a1a2afaae10189314bebdc2b124bd835602712f809689e464e984fc855221ede92302c1f22758a7bbe63fb15d7b1c1c61fe0398add596d23baabe2a9576233a4481c91fffaa88b5c03a9c3745808d2346e822df71ee2ded50ddd02d2dde0b398d9256841e239cd9c28035bb12f4de552400cb133d9b9ef2548182f87c87efe25036bc4b1b9df63414e20b0e0d3714f93277cf82f03ac3419009bfba86a280f762348ad79372329b92d53a75539d43d06e2864892bce694ec3e2f19a5b5ca0cbf24e35a5fb6e68a91e14249b76a68d2e44f73d42226153c2d529a26d6fcc1cdf260295834b0e04a80f9a29c41f0dc0071367214992d638428e8e70d81f64f5b2e5ad96678bfb14954a4b8c199306fb0158f30a49aa4690055b899af2386b4d00fa0ded4df681f41d96888c0a9fc317d52fb6c709a9d70621fe03d6d97fdc14e7843da80a004971d5932de54882fc42af2d30dfe212da3e747e3439677bd1cce5181d84f40a58f2f2cd632beb3d8fb47a450b7dc5db3da315e1658a65b2ac6686d0d9516f9d1a595796e6ef0072ac339c211e0b24ae4450212d8470decd72394caaf5f9d2a36b08fa23d8cc3dcd0555979ee93389b6f51484d5b0b559281b3b664edd175b8c6737d85f8e887db35a427c48418b2d61ba6ae178eddee81b3d530b4923202e49ad277a6ec0041497 '
    print(decrypt_sentences([sss, sss], True))
