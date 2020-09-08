import os
import sys
sys.path.append(os.environ['PUSHPATH'])
from center_three.voice_label.utils import Read_From_Mysql,Insert_Into_Mysql,table_data
from center_three.voice_label.config import *
import pandas as pd
import numpy as np
import requests
import time
import re
from datetime import datetime
from pandas.tseries.offsets import Hour, Minute
from  itertools import chain


class Get_Remarkdata(object):

    def __init__(self):
        self.read = Read_From_Mysql()  # 创建读取mysql连接
        self.insert = Insert_Into_Mysql()  # 创建读取mysql连接

    def get_basic_data(self, timestamp):
        """
        获取 busID与CustID
        :param timestamp: 时间戳(秒)
        :return:  dataframe
        """
        table_name =insert_table.format(table_data(timestamp))
        now_time = self.table_data(timestamp)
        now_time_1hour = str(pd.to_datetime(now_time)-Hour(1))
        # now_time_1hour = str(pd.to_datetime(now_time) - Minute(10))
        # sql = "select * from {} where status = 0 and updated_at is null and call_time <= '{}'".format(table_name,now_time_1hour)
        sql = "select * from {} where status = 0 and updated_at is null and call_time <= '{}' limit 500".format(table_name, now_time_1hour)
        df = self.read.select_from_table(insert_database, sql)
        columns_list = self.read.column_from_mysql(insert_database, table_name)
        if len(df) != 0:
            df.columns = columns_list
            df = df[["id", "pid", "call_time", "businessId", "customerId", "dmlabel"]]
            return df
        else:
            return pd.DataFrame()

    def get_regex(self, type_code):
        """
        获取备注的regex dataframe
        :param type_code: 业态code 可以为""
        :return: dataframe
        """
        table_name = Get_Regex_Table

        # SQL = "select * from {} where sid in {} ".format(table_name, sid_list)
        SQL = """select * from {} where sid in
                        (select id from {} where pid = 1 and deleted_at is null)
                        and deleted_at is null and is_client = 0
                        """.format(Get_Regex_Table, label_tabel_index)
        rd = self.read
        DF = rd.select_from_table(Res_Data_Base, SQL)
        if len(DF) != 0:
            DF.columns = rd.column_from_mysql(Res_Data_Base, table_name)
            DF = DF[DF.deleted_at.isnull()]
            if len(DF) != 0:
                # TODO sid归类
                type_all = DF[(DF.yt_pcode == "")]
                type_all = type_all[["id", "sid", "yt_unique", "question_word",
                                     "answer_word",
                                     "question_result", "answer_result"]]

                type_all.columns = ["ylid", "sid", "yt_unique", "question_word",
                                     "answer_word",
                                     "question_result", "answer_result"]
                if type_code != "":
                    # TODO sid归类
                    type_df = DF[(DF.yt_pcode == type_code)]
                    if len(type_df) != 0:
                        type_df = type_df[["id", "sid", "yt_unique", "question_word",
                                           "answer_word", "question_result", "answer_result"]]
                        type_df.columns = ["ylid", "sid", "yt_unique", "question_word",
                                            "answer_word",
                                            "question_result", "answer_result"]
                        df = pd.concat([type_all, type_df])
                        df = df.reset_index(drop=True)
                    else:
                        df = type_all
                        df = df.reset_index(drop=True)
                else:
                    df = type_all
                    df = df.reset_index(drop=True)
            else:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        return df

    def get_post_data(self, businessId, custommerId, userid, createTime, endCreateTime):
        """
        访问iboss接口获得接口返回数据
        :param businessId: businessId
        :param custommerId: custommerId
        :param userid: pid对应的createID
        :param createTime: 打电话的时间calltime
        :param endCreateTime: calltime+1H
        :return: contents, status, unstandard, remark_id
        """
        if ((businessId != None) & (custommerId != None) & (userid != 0 ) & (createTime != None) & (endCreateTime != None)):
            createtime = pd.to_datetime(createTime)
            endcreatetime = pd.to_datetime(endCreateTime)
            payload = {
                "tableName": "bus_business",
                "tableId": str(businessId),
                "customerId": str(custommerId),
                "userId": str(userid),
                "startTime": createTime,
                "endTime": endCreateTime,
                "allList": True,
                "remarkType": "BUS_3"
            }
            token_response = requests.post(uri, data=payload)
            token_data = token_response.json()
            data = dict(token_data)
            data = data["data"]["data"]
            contents = []
            unstandard = 0
            status = 1
            remark_id = []
            for i in data:
                try:
                    content = i["content"]  # 备注内容
                    create_remark_time = i["createTime"]  # 备注时间
                    create_remark_time = pd.to_datetime(str(create_remark_time))
                    # createrId = i["createrId"]  # 创建人
                    remarkid = i["id"]
                    # status = 1
                    if (create_remark_time >= createtime) & (endcreatetime >= create_remark_time):
                        contents.append(content)
                        remark_id.append(remarkid)
                except:
                    continue
            if len(contents) == 0:
                unstandard = 1

                # create_remark_time = pd.to_datetime(create_remark_time)  # 备注时间
                # create_call_time = pd.to_datetime(createTime)  # 电话时间
                # # TODO 看接口调整
                # end_time = create_call_time+Hour(1)
                # if (create_remark_time-create_call_time <= Hour(1)) & (createrId == id):
                #     contents.append(content)
                #     remark_id.append(remarkid)
                # if (create_remark_time-create_call_time > Hour(1)) & (createrId == id):
                #     unstandard = 1

            remark_id = ",".join(list(map(str, remark_id)))
            # unstandard = list(set(unstandard))[0]
            contents = ",".join(list(map(str, contents)))

            return contents, status, unstandard, remark_id
        else:
            contents, status, unstandard, remark_id = ("未获取传入接口相应数据", 1, 0, "")
            return contents, status, unstandard, remark_id

#TODO 合表之后未检查SQL以及表名需要检查
    def basic_match_post(self, df, timestamp):
        """
        根据基础busID与CustID和CreateID访问iboss接口获得备注数据
        :param df: basic基础数据dataframe
        :param timestamp: 时间戳(秒)获取对应年月表
        :return:补充相关备注数据的dataframe
        """
        id_pid_df = pd.DataFrame()
        pids = set(df.pid)
        table_name = insert_table.format(table_data(timestamp))
        for i in pids:
            SQL = "select pid,user_id from {} where pid = '{}'".format(table_name, i)
            idpid = self.read.select_from_table(insert_database, SQL)
            id_pid_df = pd.concat([id_pid_df, idpid], axis=0)
        if len(id_pid_df) != 0:
            id_pid_df.columns = ["pid", "userid"]
            id_pid_df = id_pid_df.groupby("pid").agg("last").reset_index()
            id_pid_df.columns = ["pid", "userid"]
            df_user = pd.merge(df, id_pid_df, "left", left_on="pid", right_on="pid")
        else:
            return id_pid_df

        df_user["content"] = ""
        df_user["status"] = ""
        df_user["unstandard"] = ""
        df_user["remark_id"] = ""
        value = df_user.values


        num = len(df_user)
        for i in range(num):
            call_time = value[i, 2]
            businessId = value[i, 3]
            customerId = value[i, 4]
            userid = value[i, 6]
            if call_time != None:
                start_time =self.post_data(pd.to_datetime(call_time))
                end_time = self.post_data(pd.to_datetime(call_time)+Hour(1))
                value[i, 7], value[i, 8], value[i, 9], value[i, 10] = self.get_post_data(businessId, customerId, userid, start_time, end_time)
            else:
                value[i, 8] = 1
        data = pd.DataFrame(value)
        data.columns = ["id", "pid", "call_time", "businessId", "customerId", "dmlabel", "userid", "content", "status", "unstandard", "remark_id"]
        return data

# TODO 未验证
    def match_remark(self, data_df, regex_df):
        """
        对访问接口后的数据进行备注正则匹配
        :param data_df: basic_match_post返回的dataframe
        :param regex_df: 正则的regex_dataframe
        :return: 更新basic数据表的数据
        """
        # data_df = data_df[data_df.status != 1]
        data_df["sid"] = ""
        data_df["ylid"] = ""
        data_df["remark_zresult"] = ""
        data_df["dmlabel"] =""

        regex_value = regex_df.values
        data_value = data_df.values

        data_num = len(data_df)
        regex_nums = len(regex_df)
        for i in range(data_num):
            content = data_value[i, 7]
            sid_list = []
            ylid_list = []
            result_list = []
            label_list = []
            unstandard = data_value[i, 9]
            if unstandard == 1:
                data_value[i, 5] = ["无备注"]
                data_value[i, 11] = [0]
                data_value[i, 12] = [0]
                data_value[i, 13] = [""]
                continue
            elif (content == "未获取传入接口相应数据") & (unstandard == 0):
                data_value[i, 5] = ["无法获取备注数据"]
                data_value[i, 11] = [0]
                data_value[i, 12] = [0]
                data_value[i, 13] = [""]
                continue
            for regex_num in range(regex_nums):
                ylid = regex_value[regex_num, 0]
                sid = regex_value[regex_num, 1]
                question_word = regex_value[regex_num, 3]
                question_label = regex_value[regex_num, 5]
                match_type = re.search(question_word, content)
                if match_type != None:
                    sid_list.append(sid)
                    ylid_list.append(ylid)
                    result_list.append(match_type.group())
                    label_list.append(question_label)
# TODO 添加备注标签
            if len(label_list) != 0:
                data_value[i, 5] = label_list
                data_value[i, 11] = sid_list
                data_value[i, 12] = ylid_list
                data_value[i, 13] = result_list
            # else:
            #     data_value[i, 5] = ""
            #     data_value[i, 11] = 0
            #     data_value[i, 12] = 0
            #     data_value[i, 13] = ""
        data = pd.DataFrame(data_value)
        data.columns = ["id", "pid", "call_time", "businessId", "customerId", "dmlabel", "userid", "remark_result", "status", "unstandard", "remark_id",
                        "sid", "ylid", "remark_zresult"]

        data0 = data[data.dmlabel == ""]
        data1 = data[data.dmlabel != ""]

        data1 = pd.DataFrame({
            "id": np.repeat(data1.id.values, data1.dmlabel.str.len()),
            "pid": np.repeat(data1.pid.values, data1.dmlabel.str.len()),
            "call_time": np.repeat(data1.call_time.values, data1.dmlabel.str.len()),
            "businessId": np.repeat(data1.businessId.values, data1.dmlabel.str.len()),
            "customerId": np.repeat(data1.customerId.values, data1.dmlabel.str.len()),
            "remark_result": np.repeat(data1.remark_result.values, data1.dmlabel.str.len()),
            "dmlabel": chain.from_iterable(data1.dmlabel),
            "status": np.repeat(data1.status.values, data1.dmlabel.str.len()),
            "remark_id": np.repeat(data1.remark_id.values, data1.dmlabel.str.len()),
            "sid": chain.from_iterable(data1.sid),
            "ylid": chain.from_iterable(data1.ylid),
            "remark_zresult": chain.from_iterable(data1.remark_zresult),
        })
        data = pd.concat([data0, data1], axis=0)
        data["updated_at"] = self.table_data(time.time())
        data.call_time = data.call_time.fillna("")
        data.call_time = data.call_time.astype("str")
        data.sid = data.sid.apply(lambda x: 0 if x == "" else x)
        data.ylid = data.ylid.apply(lambda x: 0 if x == "" else x)


        return data
###########################################################################################################################

    def get_abnormal(self, df, timestamp):
        """
        返回不符合备注要求的dataframe
        :param df:  match_remark
        :param timestamp: 时间戳用于查询label表的
        :return: dataframe
        """
        df = df[df.dmlabel != ""]
        df = df[["businessId", "customerId", "pid", "dmlabel"]]

        df_type0 = df[df.dmlabel == "无备注"]
        df_type1 = df[(df.dmlabel != "无备注") & (df.dmlabel != "无法获取备注数据")]
        df_type0["type"] = 0
        df_type0 = df_type0[["businessId", "customerId", "pid", "type"]]
        df_type0["created_at"] = self.table_data(time.time())

        df_type1 = df_type1[["businessId", "customerId", "pid"]]
        df_type1.drop_duplicates(inplace=True)
        df_type1["num"] = ""
        if len(df_type1) == 0:
            #         pass
            print("df_type1")
            return df_type0
        value = df_type1.values
        num = len(df_type1)
        label_table_name = Res_Table.format(table_data(time.time()))

        for i in range(num):
            pid = value[i, 2]
            SQL = "select pid,count(answer_result) from {} where answer_result = '{}' and pid = '{}' ".format(
                label_table_name, unintention, pid)
            df_need_num = self.read.select_from_table(Regex_Data_Base, SQL)
            if len(df_need_num) != 0:
                df_need_num.columns = ["pid", "num"]
                if df_need_num.pid[0] != None:
                    value[i, 3] = df_need_num.num.values[0]
        unqualified_df = pd.DataFrame(value)
        unqualified_df.columns = ["businessId", "customerId", "pid", "num"]
        unqualified_df = unqualified_df[unqualified_df.num != ""]
        if len(unqualified_df) == 0:
            #         pass
            print("df_type0")
            return df_type0
        else:
            unqualified_df = unqualified_df[["businessId", "customerId", "pid"]]
            unqualified_df["type"] = 1
            unqualified_df["created_at"] = self.table_data(time.time())

            df_all = pd.concat([unqualified_df, df_type0], axis=0)
            #         pass
            print("df_all")
            return df_all

    def table_data(self,timestamp):
        """
        :param timestamp:  时间戳
        :return: 年月 (格式)->2020-07-01 10:10:10
        """
        dt = datetime.fromtimestamp(int(timestamp))
        date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
        return date

    def post_data(self, pandastime):
        """
        目前接口要求时间格式不确定,暂时是这个
        :param timestamp:  时间戳
        :return: 年月 (格式)->2020-07-01 10:10:10
        """
        timestamp = datetime.timestamp(pandastime)
        dt = datetime.fromtimestamp(int(timestamp))
        date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
        return date
