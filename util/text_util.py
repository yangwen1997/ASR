# coding: utf-8

import re
import codecs
import zlib


def compress_str():
    """压缩字符
    当谈起压缩时我们通常想到文件，比如ZIP结构。在Python中可以压缩长字符，不涉及任何档案文件。
    """
    string = ''
    compressed = zlib.compress(string)
    decompressed = zlib.decompress(compressed)
    print("Original Size:{0}".format(len(string)))
    print("Original Size:{0}".format(len(compressed)))
    print("Original Size:{0}".format(len(decompressed)))


def saveas_with_length(filepath, minlen=1, maxlen=10, reverse=False, is_distinct=True):
    text_list = []
    for line in codecs.open(filepath, "r", "utf-8").readlines():
        line = line.strip()
        if line and len(line) >= minlen and len(line) <= maxlen:
            text_list.append(line)
    if is_distinct:
        text_list = list(set(text_list))
    text_list.sort(key=lambda x: len(x), reverse=reverse)
    codecs.open(filepath + ".temp", "w", "utf-8").write("\n".join(text_list))


def find_out_of_bracket(item_list, content):
    """
        查找item_list中未出现在content中的内容
        一般用于案发地址的提取分类
    """
    results = []
    if re.search("[(（]", content) and re.search("[)）]", content):
        left_bracket_list = []
        for match_item in re.finditer("[（(]", content):
            start = match_item.start()
            left_bracket_list.append(start)
        right_bracket_list = []
        for match_item in re.finditer("[）)]", content):
            start = match_item.start()
            right_bracket_list.append(start)
        bracket_list = list(zip(left_bracket_list, right_bracket_list))
        for item in item_list:
            flag = 0
            try:
                for match_addr in re.finditer(item, content):
                    start_index = match_addr.start()
                    end_index = match_addr.end()
                    for bracket_tup in bracket_list:
                        if not (bracket_tup[0] < start_index and end_index <= bracket_tup[1]):
                            results.append(item)
                            flag = 1
                            break
                    if flag == 1:
                        break
            except Exception as e:
                print(item, content, item_list)
                raise Exception(e)
    else:
        results = item_list
    return results


def find_common_substr(s1, s2):
    """
        获取两个字符串的公共部分
    """
    m = [[0 for i in range(len(s2) + 1)] for j in range(len(s1) + 1)]
    # print("m",m)# 生成0矩阵，为方便后续计算，比字符串长度多了一列
    mmax = 0  # 最长匹配的长度
    p = 0  # 最长匹配对应在s1中的最后一位
    for i in range(len(s1)):
        for j in range(len(s2)):
            if s1[i] == s2[j]:
                m[i + 1][j + 1] = m[i][j] + 1
                if m[i + 1][j + 1] > mmax:
                    mmax = m[i + 1][j + 1]
                    p = i + 1
    return s1[p - mmax:p]  # 返回最长子串及其长度


def rreplace(content, old, new, *max):
    """
        从右边开始替换
    """
    count = len(content)
    if max and str(max[0]).isdigit():
        count = max[0]
    return new.join(content.rsplit(old, count))


def del_bicommom_substr(addr_list):
    """
        删除作为其他字符串的子串
    """
    for idx in range(len(addr_list)):
        for i in [i for i in range(len(addr_list)) if i != idx]:
            if addr_list[idx] in addr_list[i]:
                addr_list[idx] = ""
                break
    addr_list = [addr.strip() for addr in addr_list if len(addr.strip()) > 1]
    return addr_list


def sort_address(addr_list, content, debug=False):
    """
        对切分后的短地址进行排序
    """
    if len(addr_list) < 2:
        return addr_list
    addr_index_list = [(addr, content.find(addr))
                       for addr in addr_list if addr in content]
    if len(addr_list) != len(addr_index_list):
        print("addr_list: ", addr_list)
        print("content: ", content)
        if debug:
            raise Exception("Some address of addr_list is not in content")
        else:
            print("Some address of addr_list is not in content")
    if addr_index_list:
        addr_index_list.sort(key=lambda x: x[1])
        return list(list(zip(*addr_index_list))[0])
    else:
        return []


def is_neighbor(addr_list, content, debug=False):
    """
        判断切分后的短地址是否相邻
    """
    flag = True
    if len(addr_list) > 1:
        addr_index_list = [(addr, content.find(addr), len(addr))
                           for addr in addr_list if addr in content]
        if len(addr_list) != len(addr_index_list):
            print("addr_list: ", addr_list)
            print("content: ", content)
            if debug:
                raise Exception("Some address of addr_list is not in content")
            else:
                print("Some address of addr_list is not in content")
        if len(addr_index_list) > 2:
            addr_index_list.sort(key=lambda x: x[1])
            for i in range(1, len(addr_index_list)):
                if addr_index_list[i - 1][1] + addr_index_list[i - 1][2] == addr_index_list[i][1]:
                    flag = False
                    break
    return flag


def get_nearest_addr(addr, count=1):
    """
        利用联想法获取最近的多个地址
        注：目前仅支持 XX街XX号
    addr 地址
    count 期待返回最近的地址数
    """
    nearest_addrs = []
    if re.search("[\u4e00-\u9fa5]{2,5}街([0-9]){1,3}号", addr):
        matched_re = re.search("[\u4e00-\u9fa5]{2,5}街([0-9]{1,3})号", addr)
        hao_num = int(matched_re.group(1))
        flag = False  # 号是否到0
        curr_hao_num = 0
        previous_num = 0
        for i in range(count):
            if not flag:
                curr_hao_num = hao_num + int((-1) ** i * (i / 2 + 1))
            else:
                curr_hao_num = previous_num + 1
            if curr_hao_num:
                previous_num = curr_hao_num
            else:
                curr_hao_num = previous_num + 1
                previous_num = curr_hao_num
                flag = True
            nearest_addrs.append(re.sub("[0-9]{1,3}", str(curr_hao_num), addr))
    return nearest_addrs
