#!/usr/bin/env python
# encoding: utf-8
"""
@File    : test_func.py
@Time    : 2020/8/14 13:48
@Author  : W
@Software: PyCharm
"""
import re
import time
import os
import sys
import jieba

sys.path.append(os.environ['PUSHPATH'])


class Test_Func(object):
    """
    用于标签二次清洗
    方法必须使用统一格式:
    传入参数:
    text: 原始文本
    pattern_result: 正则匹配之后产生的文本
    输出参数
    "tag_value": 标签值域中的某个具体取值
    func_result: 函数处理完之后产生的实际取值结果
    """

    @staticmethod
    def guarantee_success(text=None, pattern_result=None):
        i = 0
        pats = ["包过来", "红包过", "承包过", "包过去"]
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                i += 1
        if i == 0:
            return '是否包过', pattern_result
        else:
            return None, None

    @staticmethod
    def time_urgent(text=None, pattern_result=None):
        if "现在" in pattern_result:
            i = 0
            pats = ["现在[^,]*?不需?[要用是]", "你现在"]
            for pat in pats:
                pattern = re.compile(pat)
                result = pattern.search(text)
                if result != None:
                    i += 1
            if i == 0:
                return '急用', pattern_result
            else:
                return None, None
        else:
            return '急用', pattern_result

    @staticmethod
    def car_value(text=None, pattern_result=None):
        tag_result, result = Test_Func.extract_money(text)
        if result != None:
            return '车辆价值', result
        else:
            return None, None

    @staticmethod
    def house_value(text=None, pattern_result=None):
        tag_result, result = Test_Func.extract_money(text)
        if result != None:
            return '房产价值', result
        else:
            return None, None

    @staticmethod
    def expensive(text=None, pattern_result=None):
        i = 0
        i = 0
        pats = ["(?!不)[高贵倍]|受不了|入不敷出", "[有能](便宜|优惠)吗?", "(便宜|优惠)[很太好得]?[多点些]"]
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                i += 1
        if i > 0:
            return '价格贵', pattern_result
        else:
            return None, None

    @staticmethod
    def charge_1(text=None, pattern_result=None):
        i = 0
        pats = ["(好多|多少)", "要[^,]*?钱[吗不]", "(什么|看|问|大概|说)[^,]*?(价格|报价|价钱|费用|手续费)"]
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                i += 1
        if i > 0:
            return '价格咨询', pattern_result
        else:
            return None, None

    @staticmethod
    def charge_2(text=None, pattern_result=None):
        i = 0
        pats = ["怎么收", "吗", "多少", "好多"]
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                i += 1
        if i > 0:
            return '价格咨询', pattern_result
        else:
            return None, None

    @staticmethod
    def receipt_no(text=None, pattern_result=None):
        pat = '(?!没|无|莫|木)有[^,]*?(借条|转账(记录)?|账单)'
        result = re.search(pat, text)
        if result == None:
            return "否", pattern_result
        else:
            return "是", result.group()

    @staticmethod
    def law_processed_answer( answer_text=None, answer_pattern=None):

        result = re.search(answer_pattern, answer_text)

        if result is not None:
            return "不需要"

        return None

    @staticmethod
    def law_processed_ask(question_result=None, ask_pattern=None):

        result = re.search(ask_pattern, question_result)
        if result is not None:
            # 过滤
            filter_str = "(法|事务所|律所|律师)"
            ret = re.search(filter_str, question_result)
            if ret is not None:
                return result

        return None

    @staticmethod
    def lisence_ensure(text=None, pattern_result=None):
        pats = ["(已[经?]|早[就都]?)(安排|[做搞办找])[好了人]{1,2}", "有[人]?[^,].{0,2}负责[的了]{1,2}"]
        i = 0
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                func_result = result.group(0)
                i += 1
        if i > 0:
            return '是', func_result
        else:
            pat_1 = re.compile("[对啊嗯有]")
            pat_2 = re.compile("是")
            result_1 = pat_1.search(text)
            result_2 = pat_2.search(text)
            if result_1 == None:
                if result_2 != None:
                    pat = "是[哪谁]"
                    pattern = re.compile(pat)
                    result = pattern.search(text)
                    if result == None:
                        return '是', '是'
                    else:
                        return None
            else:
                func_result = result_1.group(0)
                return '是', func_result

    @staticmethod
    def accused(text=None, pattern_result=None):
        if "我是原告" in text:
            return "原告", "我是原告"
        else:
            return "被告", pattern_result

    @staticmethod
    def accuser(text=None, pattern_result=None):
        if "我是被告" in text:
            return "被告", "我是被告"
        else:
            return "原告", pattern_result

    @staticmethod
    def invite_agree(text=None, pattern_result=None):
        if pattern_result == "好":
            pat = "你好|好吗|好的?[知道明白]{2}"
            result_1 = re.search(pat, text)
            if result_1 == None:
                return '同意', pattern_result
            else:
                return None, None

    @staticmethod
    def age(text, pattern_result):
        pat = '[一二三四五六七八九十两几\d]+年(出生|生)?'
        result_1 = re.search(pat, text)
        if result_1 != None:
            return '年龄', result_1.group()
        else:
            return '年龄', pattern_result

    @staticmethod
    def extract_money(text_string=None, re_result=None):

        # def extract_money(self,text_string,re_result = ""):
        """提取金额的函数"""
        # 1. 不允许的词
        head_not_allow_words = "周期第看敲瞧望猜初高大惠捅挤双"
        #     middle_not_allow_words = "少吧吗嘛这那"
        middle_not_allow_words = "少"
        not_allow_words = "年种月天小家甲箱张点辆个次公里只间栋套台条分架刀根片半会般版口期起毛周圈窝包吨斤两米厘公笔单下哈框筐端段部队列行共定样头对张桶盆些双人巴号直"

        not_allow_words = middle_not_allow_words + not_allow_words
        # 2. 提取金额的召回正则
        error_pattern = "([{}][一二三四五六七八九十两万百千亿几来多\d]+[{}]|[{}][一二三四五六七八九十两万百千亿几来多\d]+|[一二三四五六七八九十两万百千亿几来多\d]+[{}])".format(
            head_not_allow_words, not_allow_words, head_not_allow_words, not_allow_words)

        # 3. 利用findall获取所有的错误结果，并替换成 ""
        findall_result_list = re.findall(error_pattern, text_string)
        for text in findall_result_list:
            try:
                text_string = text_string.replace(text, "")
            #             print(text)
            except Exception as e:
                print("报错", e)
                continue

        # 4. 正确的正则
        pattern = "((?<![{}])([\d一二三四五六七八九十两几十百千万亿]+[几来多]?(?![{}])(万|十万|百万|千万|亿|百|千)?[元块]?))+([一二三四五六七八九十两\d]+)?".format(
            head_not_allow_words, middle_not_allow_words)

        # 5. 正则匹配的结果
        #     print(text_string)
        #     result = re.search(pattern, text_string)
        #     all_result = re.findall(pattern, text_string)
        #     print(all_result)
        #     print(result)
        #     if result is None:
        #         return None, None
        #     else:
        #         #         return result.group(),"金额"
        #         return "具体金额", result.group()

        result_list = re.findall(pattern, text_string)
        if len(result_list) == 0:
            return None, None
        else:
            final_result_string = None

            words_string = set("百千万亿块元")  # 判断的各个字
            for result_set in result_list:
                for result in result_set:
                    temp_set = set(result)
                    if len(temp_set.intersection(words_string)) != 0:
                        final_result_string = result
                    else:
                        continue
            if final_result_string is None:
                return "具体金额", result_list[0]
            else:
                return "具体金额", final_result_string
    @staticmethod
    def reject_recheck_answer_no(answer_text=None, answer_pattern=None):
        return "驳回复审", "否"

    @staticmethod
    def reject_recheck_answer_yes(answer_text=None, answer_pattern=None):
        return "驳回复审", "是"

    @staticmethod
    def scale_income_answer(answer_text=None, pattern_result=None):

        # 过滤掉 看一下
        if len(str(pattern_result)) > 1:
            # 需要过滤 11个人 11点 11天 11年等数据
            # 嗯我这个银行卡是怎么的一一个月的流水是不多的几万
            filter_pattern = pattern_result + "\w{0,3}[人|点|箱|年|月|日|天]"
            filter_ret = re.findall(filter_pattern, answer_text)
            if len(filter_ret) > 0:
                # 符合过滤条件
                for ret in filter_ret:
                    answer_text = answer_text.replace(ret, "")
                pattern_result = re.search("(?:[几|十|百|千|万|一|二|三|四|五|六|七|八|九]+)|(\d+)多?万?", answer_text)
                if pattern_result is not None:
                    return "规模资金", pattern_result
                else:
                    return None, None

            return "规模资金", pattern_result

        return None, None

    @staticmethod
    def scale_employee_answer(answer_text=None, pattern_result=None):

        # 过滤掉 看一下
        if len(str(pattern_result)) > 1:
            # 需要过滤 11个人 11点 11天 11年等数据
            # 嗯我这个银行卡是怎么的一一个月的流水是不多的几万
            filter_pattern = pattern_result + "\w{0,3}[人|点|箱|年|月|日|天]"
            filter_ret = re.findall(filter_pattern, answer_text)
            if len(filter_ret) > 0:
                # 符合过滤条件
                for ret in filter_ret:
                    answer_text = answer_text.replace(ret[0], "")
                pattern_result = re.search("([几|十|百|千|万|一|二|三|四|五|六|七|八|九]+\w{0,5}人)|(\d+\w{0,5}人)", answer_text)
                if pattern_result is not None:
                    return "规模人数", pattern_result
                else:
                    return None, None

            return "规模人数", pattern_result

        return None, None

    @staticmethod
    def abuse(text=None, pattern_result=None):
        i =0
        pats =["他妈的","操你妈",'你们[^,]*?是不是吃饱了','神经病','神经病','乱鸡巴打']
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                pattern_result = result.group()
                i+=1
        if i > 0:
            return '辱骂',pattern_result
        else:
            return None,None

    @staticmethod
    def no_disturb(text=None, pattern_result=None):
        i = 0
        pats = ["(不[要用]|别)[再给打][^,]*?电话", "你们[^,]*?在[^,]*?骚扰我","把我的电话[^,]*?[删黑]"]
        for pat in pats:
            pattern = re.compile(pat)
            result = pattern.search(text)
            if result != None:
                pattern_result = result.group()
                i += 1
        if i > 0:
            return '勿扰', pattern_result
        else:
            return None, None

    @staticmethod
    def self_no(text, pattern_result):
        pat = "自己|就是我|不是帮"
        result = re.search(pat,text)
        if result ==None:
            return '不是本人','不是本人'
        else:
            return '是本人','是本人'

    @staticmethod
    def scale_employee_ask(ask_text=None, pattern_result=None):

        # 过滤
        filter_str = "(法|律师|律所|事务所|起诉|合同|报告|他|案件)"
        ret = re.search(filter_str, ask_text)
        if ret is not None:
            return None, None

        return '人数规模', pattern_result

    @staticmethod
    def law_processing_answer(answer_text=None, pattern_result=None):

        return "是", pattern_result

    @staticmethod
    def law_processing_ask(question_text=None, pattern_result=None):

        # 过滤
        filter_str = "(法|事务所|律所|律师)"
        ret = re.search(filter_str, question_text)
        if ret is not None:
            return '案情是否解决中', pattern_result

        return None, None

    @staticmethod
    def signed_competitor_ask(ask_text=None, pattern_result=None):

        # 过滤
        filter_str = "(法|事务所|律所|律师|弃权|借条|欠款|官司|诉讼费)"
        ret = re.search(filter_str, ask_text)
        if ret is not None:
            return None, None
        return '同行收费情况', pattern_result

    @staticmethod
    def advanced_repayment_answer(answer_text=None, pattern_result=None):

        if "他" in answer_text:
            return None, None

        # 哎可以我提前还我不知道是您您是哪一个公司反正我加过你们有有一个律师事务所的一个微信
        # 呃是对方已经还钱了是吧3万多都会去还了哦好的好的那您这边还有呃那您这边现在还有其他的法律问题吗 **** 呃现在目前的问题,淘宝提前还,对对对
        # 反悔了就不同意这个价格然后就好像是不知道是想要网上提前还想起诉
        ret = re.search("(法|律师|律所|事务所|起诉|合同|违约)", answer_text)
        if ret is not None:
            return None, None
        return '是', pattern_result

    @staticmethod
    def advanced_repayment_ask(ask_text=None, pattern_result=None):
        # 过滤
        filter_str = "(法|律师|律所|事务所|起诉|合同|违约)"
        ret = re.search(filter_str, ask_text)
        if ret is not None:
            return None, None

        return '是否反问提前还款', pattern_result

    @staticmethod
    def borrow_duration_ask(ask_text=None, pattern_result=None):

        # 过滤
        filter_str = "(逾期)|(一直|多久|多长)没有(交|还)"
        ret = re.search(filter_str, ask_text)
        if ret is not None:
            print('这是逾期')
            return "逾期", pattern_result

        filter_str = "\d+年"
        ret = re.search(filter_str, ask_text)
        if ret is not None:
            ret = ret.group()
            ret = ret.replace("年", "")
            if len(ret) > 1:  # 过滤 05年 17年
                return "贷款年月日", pattern_result

        return '贷款时间', pattern_result

    @staticmethod
    def switch_job_answer(answer_text=None, pattern_result=None):

        # 过滤法律信息
        ret = re.search("(法|律师|律所|事务所|起诉|合同|报告|他)", answer_text)
        if ret is not None:
            return None, None

        if "倒闭" in pattern_result:
            # 如果通天大论的说倒闭的事情，几乎就是法律的
            if len(answer_text) > 25:
                return None, None

            # 你公司倒闭了呀
            ret = re.search("你\w{0,2}公司\w{0,2}倒闭", answer_text)
            if ret is not None:
                return None, None

        return "客户转行", pattern_result

    @staticmethod
    # 这个还要改
    def cut_yes_or_no(text):
        word_list = list(jieba.cut(text))
        if "有没有" in word_list:
            index = word_list.index("有没有")
            word_list[index] = "有"
            word_list.insert(index + 1, "没有")

        item_list = list()
        key_list = ["社保", "公积金", "保单", "信用卡", "房贷"]
        value_list = ["有", "没有"]
        for word in word_list:
            if len(item_list) == 0:
                item_dict = dict()
                item_list.append(item_dict)
            else:
                last_item = item_list[-1]
                key = list(last_item.keys())[0]
                value = last_item[key]
                if key is None or value is None:
                    item_dict = last_item
                else:
                    item_dict = dict()
                    item_list.append(item_dict)

            keys = list(item_dict.keys())
            if len(keys) == 0:
                key = None
                item_dict.setdefault(key, None)
            else:
                key = keys[0]

            if word in key_list:
                print("before", item_dict)
                #         item_dict[word] = item_dict.pop(key)

                item_dict.update({word: item_dict.pop(key)})
                print("after", item_dict)
            elif word in value_list:

                item_dict[key] = word
        return item_list

    @staticmethod
    def company_cancellation(text=None, pattern_result=None):
        no_words = ['信用卡', '征信']
        i = 0
        for word in no_words:
            if word in pattern_result:
                i += 1
        if i == 0:
            return '公司已注销', pattern_result
        else:
            return None, None

    @staticmethod
    def loan_amount(text=None, pattern_result=None):
        tag_result, result = Test_Func.extract_money(text)
        if result != None:
            return '贷款金额', result
        else:
            return None, None