#!/usr/bin/env python
# encoding: utf-8
'''
@author: 孙大力
@contact: sunxiaolong@dgg.net
@file: operate_mysql.py
@time: 2020/8/27 5:29 下午
@desc:
'''
from center_three.util.utils import ReadFromMysql, InsertIntoMysql
from center_three.config.config import *

# TODO:创建评分字段与规则表，质检标签与话术提示表
class LabelApplicationTable(InsertIntoMysql):
    def create_score_table(self, database_name):
        """创建评分规则表"""
        # 1. 先创建连接
        conn = self.getmysqlservice(database_name)
        cursor = conn.cursor()

        # 2. 建表语句
        sql = """CREATE TABLE IF NOT EXISTS `score_table` (
              `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一id,表示score_id',
              `UserId` int(11) NOT NULL COMMENT '用户id',
              `ScorePid` int(11) NOT NULL COMMENT '评分字段pid',
              `ScoreName` varchar(255) NOT NULL COMMENT '评分字段名',
              `Score` int(11) NOT NULL COMMENT '分值',
              `ScoreWeight` float(11,1) NOT NULL COMMENT '评分字段权重',
              `MinValue` float(11,0) DEFAULT NULL COMMENT '区间最小值',
              `MaxValue` float(11,0) DEFAULT NULL COMMENT '区间最大值',
              `IsContinuous` int(11) NOT NULL DEFAULT '0' COMMENT '评分字段是否是连续型，默认否',
              `IsValid` int(11) NOT NULL DEFAULT '1' COMMENT '是否有效，默认1',
              `YtCode` varchar(255) DEFAULT NULL COMMENT '所属业态',
              `CreatedAt` datetime DEFAULT NULL COMMENT '创建时间',
              `UpdatedAt` datetime DEFAULT NULL COMMENT '更新时间',
              `DeletedAt` datetime DEFAULT NULL COMMENT '删除时间',
              `ExtentChar1` varchar(255) DEFAULT NULL COMMENT '扩展char字段1',
              `ExtentChar2` varchar(255) DEFAULT NULL COMMENT '扩展char字段2',
              `ExtentInt1` int(11) DEFAULT NULL COMMENT '扩展int字段1',
              `ExtentInt2` int(11) DEFAULT NULL COMMENT '扩展int字段2',
              PRIMARY KEY (`id`) USING BTREE,
              KEY `id` (`id`) USING BTREE COMMENT '唯一id'
            ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;
        """

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def create_label_reminder(self, database_name):
        """创建标签提醒表"""
        # 1. 先创建连接
        conn = self.getmysqlservice(database_name)
        cursor = conn.cursor()

        # 2. 建表语句
        sql = """CREATE TABLE IF NOT EXISTS `label_reminder_table` (
              `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '匹配字段的唯一id',
              `Pid` int(11) DEFAULT NULL COMMENT '匹配字段的pid',
              `UserId` int(11) NOT NULL COMMENT '用户id',
              `LabelName` varchar(255) NOT NULL COMMENT '标签名',
              `RemindContent` text NOT NULL COMMENT '提醒文本',
              `IsValid` int(11) NOT NULL COMMENT '是否有效',
              `YtCode` varchar(255) DEFAULT NULL COMMENT '所属业态',
              `CreatedAt` datetime DEFAULT NULL COMMENT '创建时间',
              `UpdatedAt` datetime DEFAULT NULL COMMENT '更新时间',
              `DeletedAt` datetime DEFAULT NULL COMMENT '删除时间',
              `ExtentVar1` varchar(255) DEFAULT NULL COMMENT '扩展var字段1',
              `ExtentVar2` varchar(255) DEFAULT NULL COMMENT '扩展var字段2',
              `ExtentInt1` int(11) DEFAULT NULL COMMENT '扩展int字段1',
              `ExtentInt2` int(11) DEFAULT NULL COMMENT '扩展int字段2',
              PRIMARY KEY (`id`) USING BTREE,
              KEY `id` (`id`) USING BTREE
            ) ENGINE=InnoDB AUTO_INCREMENT=167 DEFAULT CHARSET=utf8;"""

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def create_score_result_table(self,database_name):
        """

        :param database_name:
        :return:
        """
        # 1. 先创建连接
        conn = self.getmysqlservice(database_name)
        cursor = conn.cursor()

        # 2. 建表语句
        sql = """CREATE TABLE IF NOT EXISTS `score_result` (
              `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '唯一id',
              `UserId` int(11) DEFAULT NULL COMMENT '用户id',
              `pid` text NOT NULL COMMENT '通话id',
              `unique_id` text NOT NULL COMMENT '当前说话人ID',
              `QuestionLabel` text COMMENT '问题标签',
              `QuestionMatchText` text COMMENT '问题匹配的结果',
              `QuestionOriginalText` text COMMENT '问题原始的文本',
              `AnswerLabel` text COMMENT '回答标签',
              `AnswerMatchText` text COMMENT '回答匹配的结果',
              `AnswerOriginalText` text COMMENT '回答原始文本',
              `ScoreId` int(11) NOT NULL COMMENT '分数项id',
              `ScoreName` varchar(255) NOT NULL COMMENT '评分字段',
              `ScoreValue` varchar(255) NOT NULL COMMENT '需要评分的值',
              `Score` int(11) DEFAULT NULL COMMENT '分数',
              `ScoreWeight` float(11,0) DEFAULT NULL,
              `TotalScore` float(11,0) DEFAULT NULL COMMENT '该字段总分',
              PRIMARY KEY (`id`) USING BTREE,
              KEY `id` (`id`) USING BTREE COMMENT '唯一id'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()

    def create_label_reminder_result_table(self,database_name):
        """

        :param database_name:
        :return:
        """
        # 1. 先创建连接
        conn = self.getmysqlservice(database_name)
        cursor = conn.cursor()

        # 2. 建表语句
        sql = """CREATE TABLE IF NOT EXISTS `label_remind_result` (
          `id` int(11) NOT NULL COMMENT '唯一id',
          `UserId` int(11) DEFAULT NULL COMMENT '用户id',
          `pid` text NOT NULL COMMENT '通话id',
          `unique_id` text NOT NULL COMMENT '当前说话人ID',
          `RegexId` bigint(128) DEFAULT NULL COMMENT '文本标签正则id',
          `QuestionLabel` text COMMENT '问题标签',
          `QuestionMatchText` text COMMENT '问题匹配的结果',
          `QuestionOriginalText` text COMMENT '问题原始的文本',
          `AnswerLabel` text COMMENT '回答标签',
          `AnswerMatchText` text COMMENT '回答匹配的结果',
          `AnswerOriginalText` text COMMENT '回答原始文本',
          `LabelId` int(11) NOT NULL COMMENT '匹配字段id',
          `UserId` int(11) DEFAULT NULL COMMENT '匹配字段的user_id',
          `LabelName` varchar(255) DEFAULT NULL COMMENT '标签名',
          `RemindContent` varchar(255) DEFAULT NULL COMMENT '提醒文本',
          `CallTime` datetime DEFAULT NULL COMMENT '通话时间',
          `CreateTime` datetime DEFAULT NULL COMMENT '匹配创建时间',
          PRIMARY KEY (`id`),
          KEY `id` (`id`) USING BTREE COMMENT '唯一id'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """

        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()


