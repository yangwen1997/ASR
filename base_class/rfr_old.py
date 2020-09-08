# encoding: utf-8
'''
@author: 高琴妹
@contact: gaoqinmei@dgg.net
@file: *.py
@time: date
@desc: 用于加载全量数据
'''

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import sklearn.metrics as sm
from sklearn.externals import joblib
from base_class.read_mongo import read_busfeature, read_bustrain, read_empfeature, read_employee_with_division
from util.read_database.map_tree import Division
from resource_drop.develop_drop import stopwordfile
from base_class.read_mysql import Read_From_MySQL as readMysql
from util.mapDGG import *
from sklearn.preprocessing import StandardScaler
from util.c_define import *

class BaseModel_RF(object):
    def __init__(self, p_code=None):
        print("开始初始化model: ", str(self), p_code)
        self.rftrain_data = None
        self.rfmodel = None
        self.rfemployee_data = None
        self.rfbusiness_data = None
        self.rffile_path = None
        self.busrank = None
        self.emprank = None
        if p_code is not None:
            self.business_p_code = p_code
        else:
            raise NotImplementedError("self.business_p_code必须在初始化时赋值, 用于标记模型的业态")
        # try:
        #     # 单机部署
        #     self.rf_train(file_path)
        # except:
        #     print("*****************模型训练出错*******************")
        # self.desc = 'This is BaseModel, please overwrite this property'
        try:
            print('Division().industry_division: ', Division().industry_division)
            self.division_list = Division().industry_division[p_code]
        except Exception as e:
            self.division_list = []
            print("事业部id获取失败, 通过商机业态读取train_data")

    def feature_engineering(self,buss_data):
        buss_data = buss_data.replace("\\N", np.nan)
        buss_data.fillna(0, inplace=True)

        #####衍生特特征##########
        intcols = ['follow_times', 'allot_number', 'invalid_number', 'mark_times',
                   'vip', 'is_mind', 'referral', 'noconnected_times', 'nosign_times', 'pickup_times',
                   'exceed_noconnected', 'invalid_times', 'turnover_num', 'followers', 'tccs']
        if 'label' in buss_data.columns:
            buss_data['label'] = buss_data['label'].astype(int)

        print("=======商机分类训练数据字段========：\n",buss_data.columns,buss_data.shape)
        buss_data[intcols] = buss_data[intcols].astype(int)
        buss_data['pick_nosign_times'] = buss_data["pickup_times"] - buss_data["nosign_times"]
        if "label" in buss_data.columns:
            print("正负样本量：", buss_data.label.value_counts())

        print("==============开始处理离散特征===================")
        columns = ["opportunitytype_code", 'add_type_code', 'origin_code']
        from sklearn import preprocessing
        le = preprocessing.LabelEncoder()
        for i in columns:
            buss_data[i] = buss_data[i].astype("str")
            buss_data[i].fillna("no", inplace=True)
            buss_data[i] = le.fit_transform(buss_data[i])
        print("特征工程后的字段：", buss_data.columns)
        return buss_data

    def rf_train(self, file_path=None):
        self.rffile_path = file_path
        if self.rffile_path is None:
            base_dir = os.getcwd()
            print(base_dir)
            # d = path.dirname(__file__)  #返回当前文件所在的目录
            # abspath = path.abspath(d) #返回d所在目录规范的绝对路径
            # 数据文件路径
            self.rffile_path = os.path.abspath(os.path.dirname(base_dir) + os.path.sep + ".") + '/data/'
            print("file_path is None, 设置默认值：", file_path)

        # 为了调用各业态商务商机分类结果
        # self.rffile_path = file_path
        print(str(self))
        p_code = self.business_p_code
        bustrain_path = file_path + 'bus_train{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        busfeature_path = file_path + 'bus_feature{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        emprank_path = file_path + 'emp_rank{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        rfmodel_path = file_path + 'rfr{}.pkl'.format(datetime.datetime.now().strftime('%Y%m%d'))
        is_exists_bustrain = os.path.exists(bustrain_path)
        is_exists_busfeature = os.path.exists(busfeature_path)
        is_exists_rfmodel = os.path.exists(rfmodel_path)
        is_exists_emprank = os.path.exists(emprank_path)

        if is_exists_rfmodel and is_exists_busfeature and is_exists_emprank:
            print("读取已有模型: ", str(self))
            self.rfbusiness_data = pd.read_csv(busfeature_path)
            self.rfmodel = joblib.load(rfmodel_path)
            self.rfemployee_data = pd.read_csv(emprank_path)
        else:
            print("================读取训练数据================")
            division_list = self.division_list
            try:
                if is_exists_bustrain:
                    self.rftrain_data = pd.read_csv(bustrain_path)
                else:
                    self.rftrain_data = pd.DataFrame(read_bustrain(division_list))
                    self.rftrain_data.to_csv(bustrain_path, encoding='utf-8', sep=',', index=0)
                print("商机分类训练数据", self.rftrain_data.shape)
            except Exception as e:
                print("读取训练数据出错，检查mongon数据库或", bustrain_path, is_exists_bustrain)

            print("===========读取商机数据===========")
            try:
                if is_exists_busfeature:
                    self.rfbusiness_data = pd.read_csv(busfeature_path)
                else:
                    self.rfbusiness_data = pd.DataFrame(read_busfeature(division_list))
                    self.rfbusiness_data.to_csv(busfeature_path, encoding='utf-8', sep=',', index=0)
                print("商机特征数据", self.rfbusiness_data.shape)
            except Exception as e:
                print_to_log(e, level=5)


            print("===========读取商务数据===========")
            try:
                if is_exists_emprank:
                    self.rfemployee_data = pd.read_csv(emprank_path)
                else:
                    emp_data1 = pd.DataFrame(read_empfeature())
                    # emp_data2 = pd.DataFrame(read_employee(p_code=p_code))
                    emp_data2 = pd.DataFrame(read_employee_with_division(division_list))

                    empfd = pd.merge(
                        emp_data2[["follower_id", "format_id", "average_achievement", "signbillcount", "level_3"]],
                        emp_data1, left_on="follower_id", right_on="follower_id", how="left")

                    empfd.to_csv(emprank_path, encoding='utf-8', sep=',', index=0)
                    self.rfemployee_data = empfd
                print("商务特征数据", self.rfemployee_data.shape)
            except Exception as e:
                print_to_log(e, level=5)


            trainfeature = self.feature_engineering(self.rftrain_data)
            print("================开始采样================")
            x = trainfeature[trainfeature.label == 0]
            x1 = x[:len(trainfeature[trainfeature['label'] == 1]) * 5]
            train_data = pd.concat([x1, trainfeature[trainfeature.label == 1]], axis=0)
            y = np.array(train_data["label"])

            X_train, X_test, y_train, y_test = train_test_split(train_data, y, test_size=0.3)
            selected_features = ['follow_times', 'mark_times', 'ordercount', 'vip', 'is_mind', 'referral', 'opportunitytype_code',
                                 'add_type_code', 'origin_code', 'noconnected_times', 'nosign_times', 'pickup_times',
                                 'exceed_noconnected', 'invalid_times', 'turnover_num', 'followers', 'tccs',
                                 "pick_nosign_times"]

            print("测试的正样本", list(y_test).count(1))
            print("训练的正样本", list(y_train).count(1))

            rfr = RandomForestRegressor(min_samples_split=20, n_estimators=800)
            # rfr = GradientBoostingRegressor()
            rfr.fit(X_train[selected_features], y_train)
            y_predict = rfr.predict(X_test[selected_features])

            print("预测出的正样本", len(y_predict[y_predict >= 0.1]))
            print("真实的正样本", list(y_test).count(1))
            mae = sm.mean_absolute_error(y_test, y_predict)
            mse = sm.mean_squared_error(y_test, y_predict)
            median = sm.median_absolute_error(y_test, y_predict)
            explain = sm.explained_variance_score(y_test, y_predict)
            R2 = sm.r2_score(y_test, y_predict)
            print("============模型效果打印===========")
            print("mae:", mae)
            print("mse:", mae)
            print("median:", median)
            print("explain:", explain)
            print("R2:", R2)

            importances = rfr.feature_importances_
            indices = np.argsort(importances)[::-1]
            for f in range(X_train[selected_features].shape[1] - 1):
                # 给予10000颗决策树平均不纯度衰减的计算来评估特征重要性
                print("%2d) %-*s %f" % (f + 1, 30, X_train[selected_features].columns[f], importances[indices[f]]))

            # 模型保存
            joblib.dump(rfr, rfmodel_path)
            self.rfmodel = rfr

    def rf_predict(self, file_path):
        print("==================rf_predict 开始商机分类==================\n", "业态：",self.business_p_code,"\n从mongon读取的商机量：", self.rfbusiness_data.shape[0])
        data = self.rfbusiness_data
        try:
            buspre_data = self.get_predata(data = data)
        except Exception as e:
            print(e,"\n从MySQL读取商机出错,或许该事业部没有待推荐的商机！",str(self))
            buspre_data = pd.DataFrame()

        if len(buspre_data)>0:
            busf_data = self.feature_engineering(buspre_data)
            print("进入模型的数据",busf_data.shape)
            # 进入模型的特征
            selected_features = ['follow_times', 'mark_times', 'ordercount', 'vip', 'is_mind', 'referral',
                                 'opportunitytype_code',
                                 'add_type_code', 'origin_code', 'noconnected_times', 'nosign_times', 'pickup_times',
                                 'exceed_noconnected', 'invalid_times', 'turnover_num', 'followers', 'tccs',
                                 "pick_nosign_times"]

            y_predict = self.rfmodel.predict(busf_data[selected_features])
            buspre_data["pre"] = y_predict
            # 开始评分
            score_df = pd.read_csv(stopwordfile)
            score_dict = score_df.set_index('content').T.to_dict('records')[0]

            result_prescore = self.content_score(buspre_data, score_dict)
            bus_rankresult = self.bus_rank(result_prescore)
            # 保存商机分类结果
            busrank_path = file_path + 'busrank_result{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
            bus_rankresult.to_csv(busrank_path, encoding='utf-8', sep=',', index=0)
        elif len(buspre_data)==0:
            print("从MySQL获取的商机与商机特征合并后为0。")

        print("================开始分类商务================")
        emprank = self.emp_rank(self.rfemployee_data)
        emprank_path = file_path + 'emprank_result{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        emprank.to_csv(emprank_path, encoding='utf-8', sep=',', index=0)
        print("================商务,商机分类完成！================")


    def content_score(self,data,score_dict):
        def getsore(b, score_dict):
            score = 5
            for i in score_dict.keys():
                if i in b:
                    score += score_dict[i]
                if score < 0:
                    score = 0
            return score

        data["last_follow_record_content"].fillna("无", inplace=True)
        data["last_follow_record_content"] = data["last_follow_record_content"].astype("str")
        data['score'] = data["last_follow_record_content"].apply(lambda x: getsore(x, score_dict))
        return data

    def bus_rank(self,bus_pre_score):
        bus_pre_score["pre"] = bus_pre_score["pre"].astype(float)
        bus_pre_score["score"] = bus_pre_score["score"].astype(float)
        print("商机分类成单预测结果分布：",bus_pre_score["pre"].describe())
        a = np.quantile(bus_pre_score["pre"],0.2)
        bus = bus_pre_score[(bus_pre_score.pre > a) | ((bus_pre_score.score) > 4)]
        cols = ["score", "pre","tccs","referral"]
        for i in cols:
            bus[i].fillna(0,inplace =True)
            bus[i] = StandardScaler().fit_transform(bus[[i]])
            bus[i] = bus[i].astype(float)
        bus["prescore"] = 0.3*bus["score"] + 0.5*bus["pre"]+0.1*bus["referral"]-0.1*bus["tccs"]
        bus_pre = bus.sort_values(by='prescore', axis=0, ascending=False).reset_index(drop=True)
        bus_pre.loc[:int(len(bus_pre) * 0.2), "rank"] = "A"
        bus_pre.loc[int(len(bus_pre) * 0.2):int(len(bus_pre) * 0.4), "rank"] = "B"
        bus_pre.loc[int(len(bus_pre) * 0.4):int(len(bus_pre) * 0.6), "rank"] = "C"
        bus_pre.loc[int(len(bus_pre) * 0.6):, "rank"] = "D"

        print("================各类商机占比==================\n", bus_pre["rank"].value_counts() / len(bus_pre))
        return bus_pre

    def emp_rank(self,empdata):
        cols = ["call_rate", "average_achievement", "signbillcount"]
        for i in cols:
            empdata[i] = empdata[i].fillna(0)
            empdata[i] = StandardScaler().fit_transform(empdata[[i]])
            empdata[i] = empdata[i].astype(float)

        empdata["score"] = 0.4 * empdata["call_rate"] + 0.4 * empdata["signbillcount"] + 0.2 * empdata[
            "average_achievement"]
        t = empdata
        result = pd.DataFrame()
        for i in set(t.level_3):
            df = t[t.level_3 == i].sort_values(by="score", ascending=False).reset_index()
            df.loc[:int(len(df) * 0.2), "rank"] = "A"
            df.loc[int(len(df) * 0.2):int(len(df) * 0.5), "rank"] = "B"
            df.loc[int(len(df) * 0.5):int(len(df) * 0.8), "rank"] = "C"
            df.loc[int(len(df) * 0.8):, "rank"] = "D"
            result = pd.concat([result, df], axis=0)
        return result

    def get_predata(self,data):
        # 从mysql获取所有待推荐的商机
        division_list = self.division_list
        db = readMysql().getAllBusFrommysql()
        cursor = db.cursor()
        sql = "select distinct business_id from dgg_resource_result_temp where division_id in (%s)" %(",".join(division_list))
        print("从mysql获取所有待推荐的商机,sql语句：",sql)
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(list(results))==0:
            print("============从mysql获取所有待推荐的商机个数为0，不做分类进行下一个业态。=========")
            buspre = pd.DataFrame()
        else:
            print("从mysql获取所有待推荐的商机个数：",len(list(results)))
            busdf = pd.DataFrame(list(results))
            busdf.columns = ["business_id"]
            busdf['business_id'] = busdf['business_id'].astype("int64")
            data.id.fillna(0,inplace=True)
            data['business_id'] = data['id'].astype("int64")
            data.drop("id",axis=1,inplace =True)
            buspre = pd.merge(busdf, data, on="business_id", how="left")
        return buspre

    def rf_rankresult(self, file_path=None):
        self.rffile_path = file_path
        if self.rffile_path is None:
            base_dir = os.getcwd()
            print(base_dir)
            # d = path.dirname(__file__)  #返回当前文件所在的目录
            # abspath = path.abspath(d) #返回d所在目录规范的绝对路径
            # 数据文件路径
            self.rffile_path = os.path.abspath(os.path.dirname(base_dir) + os.path.sep + ".") + '/data/'
            print("file_path is None, 设置默认值：", file_path)
        try:
            busrank_path = self.rffile_path + 'busrank_result{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
            print("=========商机分类结果保存路径=========", busrank_path)
            self.busrank = pd.read_csv(busrank_path)
            emprank_path = self.rffile_path + 'emprank_result{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
            self.emprank = pd.read_csv(emprank_path)
        except Exception as e:
            busrank_path = self.rffile_path + 'busrank_result{}.csv'.format(
                (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
            emprank_path = self.rffile_path + 'emprank_result{}.csv'.format(
                (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
            if os.path.exists(emprank_path):
                print("获取当天商机分类结果失败！使用前一天结果。")
                self.emprank = pd.read_csv(emprank_path)
                if os.path.exists(busrank_path):
                    print("获取当天商务分类结果失败！使用前一天结果。")
                    self.busrank = pd.read_csv(busrank_path)
                else:
                    print("没有任何商机分类结果文件！！！")
            else:
                print("没有任何商务分类结果文件！！！")
        return self.busrank, self.emprank


if __name__ == "__main__":
    # rfr_model = BaseModel_RF(p_code=smallyt_develop)
    # rfr_model.rf_train(file_path = data_path)
    # rfr_model.rf_predict(file_path = data_path)
    p_code = smallyt_develop
    division_list = Division().industry_division[p_code]
    train_data = pd.DataFrame(read_bustrain(division_list))
    print(train_data.shape)
    print(train_data.head(5))
