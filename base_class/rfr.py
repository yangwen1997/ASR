# encoding: utf-8
'''
@author: 高琴妹
@contact: gaoqinmei@dgg.net
@file: *.py
@time: date
@desc: 用于加载全量数据
'''
import warnings

warnings.filterwarnings("ignore")
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import sklearn.metrics as sm
from sklearn.externals import joblib
from base_class.read_mongo import *
from util.read_database.map_tree import Division
from resource_drop.develop_drop.config.path import stopwordfile
from base_class.read_mysql import Read_From_MySQL as readMysql
from util.mapDGG import *
from sklearn.preprocessing import StandardScaler
from util.c_define import *
from sklearn import preprocessing
from resource_drop.law_drop.data_cleaning.score_cal import deal_lyqdlb, deal_vip, deal_bus_allot, \
    deal_avgcallduration, deal_calltimes, deal_cull, deal_follow_60, deal_followtimes, deal_invitation, deal_leave, \
    deal_mark, deal_sign, deal_valid, deal_turnover

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
        #商机离线评分保存
        self.buspre_data = None
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

    def feature_engineering(self, buss_data):
        ## 分割数据来源
        buss_data = buss_data.replace("\\N", np.nan)
        buss_data.fillna(0,inplace = True)
        orderbus = buss_data
        floatcols = ["bus_cull","bus_cull_direct","bus_mark_content_num","bus_mark_phone_num",
                  "bus_follow_60s", "bus_allot", "turnover_num", "bus_leave",
                  "bus_invitation_num", "bus_calltimes"]

        orderbus[floatcols] = orderbus[floatcols].astype("float")

        #####衍生特特征##########
        orderbus["bus_culls"] = orderbus["bus_cull"] + orderbus["bus_cull_direct"]
        orderbus['contents_num'] = orderbus["bus_mark_content_num"] - orderbus["bus_mark_phone_num"]

        print("==============开始分箱处理连续特征===================")

        continuous_feature = ["bus_follow_60s", "bus_allot", "turnover_num", "contents_num", "bus_leave",
                              "bus_invitation_num", "bus_calltimes"]
        # 个特征分箱节点
        splitlist = {
            "bus_follow_60s": [5, 8, 11, 20],
            "bus_allot": [1, 3, 6, 9],
            "turnover_num": [3, 7, 11, 14],
            "contents_num": [2, 6, 11, 30],
            "bus_leave": [2, 4, 8, 9],
            "bus_invitation_num": [1, 4, 6, 8],
            "bus_calltimes": [5, 7, 12, 20]
        }

        def getpoints(x, split_list):
            if x < split_list[0]:
                return 1
            elif (x >= split_list[0]) & (x < split_list[1]):
                return 2
            elif (x >= split_list[1]) & (x < split_list[2]):
                return 4
            elif (x >= split_list[2]) & (x < split_list[3]):
                return 3
            else:
                return 2

        def splitfeature_part(data, cols):
            for col in cols:
                if col in data.columns:
                    data[col].fillna(0, inplace=True)
                    data[col].replace("nan", 0, inplace=True)
                    data[col] = data[col].astype("float")

                    split_list = splitlist[col]
                    data[col] = data[col].apply(lambda x: getpoints(float(x), split_list))
            return data

        orderbusf = splitfeature_part(orderbus, continuous_feature)

        print("==============开始编码离散特征===================")
        le = preprocessing.LabelEncoder()
        orderbusf["lyqdlb"] = orderbusf["lyqdlb"].astype("str")
        orderbusf["lyqdlb"].fillna("no", inplace=True)
        orderbusf["lyqdlb"] = le.fit_transform(orderbusf["lyqdlb"])
        return orderbusf

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
                    # 只选择order表的数据
                    self.rftrain_data = self.rftrain_data[self.rftrain_data.ly == 'order']
                    self.rftrain_data.to_csv(bustrain_path, encoding='utf-8', sep=',', index=0)
                print("商机分类训练数据", self.rftrain_data.shape)
            except Exception as e:
                print("读取训练数据出错，检查mongon数据库或", bustrain_path, is_exists_bustrain)

            print("===========读取商机数据===========")
            try:
                if is_exists_busfeature:
                    self.rfbusiness_data = pd.read_csv(busfeature_path)
                else:
                    self.rfbusiness_data = pd.DataFrame(read_busfeature(division_list,p_code=p_code))
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

            print("================开始采样================")
            print("：\n", self.rftrain_data["label"].value_counts())
            try:
                self.rftrain_data["label"] = self.rftrain_data["label"].astype("int")
                orderbus_0 = self.rftrain_data[self.rftrain_data.label == 0].sample(
                    len(self.rftrain_data[self.rftrain_data.label == 1]) * 8, replace=0, axis=0)
                orderbus_1 = self.rftrain_data[self.rftrain_data.label == 1]
                orderbus = pd.concat([orderbus_0, orderbus_1])
            except:
                print("正负样本比例超过1：8,不进行采样")
                orderbus = self.rftrain_data

            print("采样后：\n", orderbus.label.value_counts())

            print("================开始特征工程================")
            train_data = self.feature_engineering(orderbus)
            y = np.array(train_data["label"])

            X_train, X_test, y_train, y_test = train_test_split(train_data, y, test_size=0.3)
            selected_features = ["bus_invitation_num", "referral",
                                 "vip", "contents_num", "bus_allot",
                                 "turnover_num", "bus_follow_60s",
                                 "bus_leave", "bus_valid",
                                 "bus_mark_content_num", "bus_culls",
                                 "bus_calltimes", 'lyqdlb']

            print("测试的正样本", list(y_test).count(1))
            print("训练的正样本", list(y_train).count(1))

            rfr = RandomForestRegressor(min_samples_split=30, n_estimators=800)
            # rfr = GradientBoostingRegressor()
            rfr.fit(X_train[selected_features], y_train)
            y_predict = rfr.predict(X_test[selected_features])

            result = pd.DataFrame(y_test)
            result.columns = ["label"]
            result['pre'] = y_predict

            print("预测对的正样本比例", len(result[(result.label == 1) & (result.pre >= 0.1)]) / len(result[result.label == 1]))
            print("预测错的正样本比例", len(result[(result.label == 1) & (result.pre < 0.1)]) / len(result[result.label == 1]))
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
            for f in range(X_train[selected_features].shape[1]):
                # 给予10000颗决策树平均不纯度衰减的计算来评估特征重要性
                print("%2d) %-*s %f" % (f + 1, 30, X_train[selected_features].columns[f], importances[indices[f]]))

            # 模型保存
            joblib.dump(rfr, rfmodel_path)
            self.rfmodel = rfr

    def rf_predict(self, file_path):
        print("==================rf_predict 开始商机分类==================\n", "业态：", self.business_p_code,
              "\n从mongon读取的商机量：", self.rfbusiness_data.shape[0])
        data = self.rfbusiness_data
        try:
            self.buspre_data = self.get_predata(data=data)
        except Exception as e:
            print(e, "\n从MySQL读取商机出错,或许该事业部没有待推荐的商机！", str(self))
            self.buspre_data = pd.DataFrame()

        if len(self.buspre_data) > 0:
            busf_data = self.feature_engineering(self.buspre_data)
            print("进入模型的数据", busf_data.shape)
            # 进入模型的特征
            selected_features = ["bus_invitation_num", "referral",
                                 "vip", "contents_num", "bus_allot",
                                 "turnover_num", "bus_follow_60s",
                                 "bus_leave", "bus_valid",
                                 "bus_mark_content_num", "bus_culls",
                                 "bus_calltimes", 'lyqdlb']

            y_predict = self.rfmodel.predict(busf_data[selected_features])
            self.buspre_data["pre"] = y_predict
            # 开始评分
            score_df = pd.read_csv(stopwordfile)
            score_dict = score_df.set_index('content').T.to_dict('records')[0]

            result_prescore = self.content_score("bus_mark_content",self.buspre_data, score_dict,)
            bus_rankresult = self.bus_rank(result_prescore)
            # 保存商机分类结果
            busrank_path = file_path + 'busrank_result{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
            bus_rankresult.to_csv(busrank_path, encoding='utf-8', sep=',', index=0)
        elif len(self.buspre_data) == 0:
            print("从MySQL获取的商机与商机特征合并后为0。")

        print("================开始分类商务================")
        emprank = self.emp_rank(self.rfemployee_data)
        emprank_path = file_path + 'emprank_result{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        emprank.to_csv(emprank_path, encoding='utf-8', sep=',', index=0)
        print("================商务,商机分类完成！================")

    def content_score(self,colname,data,score_dict):
        def getsore(b, score_dict):
            score = 5
            for i in score_dict.keys():
                if i in b:
                    score += score_dict[i]
                if score < 0:
                    score = 0
            return score

        data[colname].fillna("无", inplace=True)
        data[colname] = data[colname].astype("str")
        data['score'] = data[colname].apply(lambda x: getsore(x, score_dict))
        return data

    def bus_rank(self,bus_pre_score):
        bus_pre_score[["bus_cull","bus_cull_direct","bus_allot"]] = bus_pre_score[["bus_cull","bus_cull_direct","bus_allot"]].astype(float)
        bus_pre_score["bus_culls"] = bus_pre_score["bus_cull"] + bus_pre_score["bus_cull_direct"]

        bus_pre_score["pre"] = bus_pre_score["pre"].astype(float)
        bus_pre_score["score"] = bus_pre_score["score"].astype(float)
        print("商机分类成单预测结果分布：", bus_pre_score["pre"].describe())
        a = np.quantile(bus_pre_score["pre"], 0.2)
        bus = bus_pre_score[(bus_pre_score.pre > a) | ((bus_pre_score.score) > 4)]
        cols = ["score", "pre", "bus_allot", "bus_culls"]
        for i in cols:
            bus[i].fillna(0, inplace=True)
            bus[i] = StandardScaler().fit_transform(bus[[i]])
            bus[i] = bus[i].astype(float)
        bus["prescore"] = 0.4 * bus["score"] + 0.3 * bus["pre"] - 0.1 * bus["bus_culls"] - 0.2 * bus["bus_allot"]
        bus_pre = bus.sort_values(by='prescore', axis=0, ascending=False).reset_index(drop=True)
        bus_pre.loc[:int(len(bus_pre) * 0.2), "rank"] = "A"
        bus_pre.loc[int(len(bus_pre) * 0.2):int(len(bus_pre) * 0.4), "rank"] = "B"
        bus_pre.loc[int(len(bus_pre) * 0.4):int(len(bus_pre) * 0.6), "rank"] = "C"
        bus_pre.loc[int(len(bus_pre) * 0.6):, "rank"] = "D"
        bus_pre_score.loc[(bus_pre_score.pre <= a) & ((bus_pre_score.score) <= 4),"rank"] = "E"
        buspre_e = bus_pre_score[bus_pre_score["rank"]=="E"]
        bus_prerank = pd.concat([bus_pre,buspre_e])


        print("================各类商机占比==================\n", bus_prerank["rank"].value_counts() / len(bus_prerank))
        return bus_prerank

    def emp_rank(self, empdata):
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

    def get_predata(self, data):
        # 从mysql获取所有待推荐的商机
        division_list = self.division_list
        db = readMysql().getAllBusFrommysql()
        cursor = db.cursor()
        sql = "select distinct business_id from dgg_resource_result_temp where division_id in (%s)" % (
            ",".join(division_list))
        print("从mysql获取所有待推荐的商机,sql语句：", sql)
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(list(results)) == 0:
            print("============从mysql获取所有待推荐的商机个数为0，不做分类进行下一个业态。=========")
            buspre = pd.DataFrame()
        else:
            print("从mysql获取所有待推荐的商机个数：", len(list(results)))
            busdf = pd.DataFrame(list(results))
            busdf.columns = ["business_id"]
            busdf['business_id'] = busdf['business_id'].astype("int64")
            data.business_id.fillna(0, inplace=True)
            data['business_id'] = data['business_id'].astype("int64")
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

    def getscore(self,data):
        print("==============开始商机评分==============")
        # data = self.rfbusiness_data
        # data["business_id"] = data["business_id"].astype("str")
        # data = data[data['business_id'] == str(business_id)]
        ######################调用商机评分规则#########################
        if 'lyqdlb' in data.columns:
            data.lyqdlb.fillna('缺失', inplace=True)
            data['lyqdlb'] = data['lyqdlb'].apply(lambda x: deal_lyqdlb(x))
        if 'vip' in data.columns:
            data.vip.fillna(-1, inplace=True)
            data['vip'] = data['vip'].apply(lambda x: deal_vip(int(x)))
        # if 'first_drop' in data.columns:
        #     data.first_drop.fillna(-1, inplace=True)
        #     data['first_drop'] = data['first_drop'].apply(lambda x: deal_first_drop(x))
        # if 'last_drop' in data.columns:
        #     data.last_drop.fillna(-1, inplace=True)
        #     data['last_drop'] = data['last_drop'].apply(lambda x: deal_last_drop(x))
        if 'bus_allot' in data.columns:
            data.bus_allot.fillna(-1, inplace=True)
            data['bus_allot'] = data['bus_allot'].apply(lambda x: deal_bus_allot(int(x)))
        if 'bus_avgcallduration' in data.columns:
            data.bus_avgcallduration.fillna(-1, inplace=True)
            data['bus_avgcallduration'] = data['bus_avgcallduration'].apply(lambda x: deal_avgcallduration(float(x)))
        if 'bus_calltimes' in data.columns:
            data.bus_calltimes.fillna(-1, inplace=True)
            data['bus_calltimes'] = data['bus_calltimes'].apply(lambda x: deal_calltimes(int(x)))
        if ('bus_cull' in data.columns) & ('bus_cull_direct' in data.columns):
            data['bus_cull'].fillna(-1,inplace=True)
            data['bus_cull_direct'].fillna(-1,inplace=True)
            data['bus_cull'] = data['bus_cull'].astype("int")
            data['bus_cull_direct'] = data['bus_cull_direct'].astype("int")
            data['bus_cull_num'] = data['bus_cull'] + data['bus_cull_direct']
            data.bus_cull_num.fillna(-1, inplace=True)
            data['bus_cull_num'] = data['bus_cull_num'].apply(lambda x: deal_cull(int(x)))
        if 'bus_follow_60s' in data.columns:
            data.bus_follow_60s.fillna(-1, inplace=True)
            data['bus_follow_60s'] = data['bus_follow_60s'].apply(lambda x: deal_follow_60(int(x)))
        if 'bus_followtimes' in data.columns:
            data.bus_followtimes.fillna(-1, inplace=True)
            data['bus_followtimes'] = data['bus_followtimes'].apply(lambda x: deal_followtimes(int(x)))
        if 'bus_invitation_num' in data.columns:
            data.bus_invitation_num.fillna(-1, inplace=True)
            data['bus_invitation_num'] = data['bus_invitation_num'].apply(lambda x: deal_invitation(int(x)))
        if 'bus_leave' in data.columns:
            data.bus_leave.fillna(-1, inplace=True)
            data['bus_leave'] = data['bus_leave'].apply(lambda x: deal_leave(int(x)))
        if ('bus_mark_content_num' in data.columns) & ('bus_mark_phone_num' in data.columns):
            data.bus_mark_content_num.fillna(-1, inplace=True)
            data.bus_mark_phone_num.fillna(-1, inplace=True)
            data['bus_mark_content_num'] =data['bus_mark_content_num'] .astype(float)
            data['bus_mark_phone_num'] = data['bus_mark_phone_num'].astype(float)
            data['bus_mark_num'] = data['bus_mark_content_num'] - data['bus_mark_phone_num']
            data['bus_mark_num'] = data['bus_mark_num'].apply(lambda x: deal_mark(int(x)))
        if 'bus_sign' in data.columns:
            data.bus_sign.fillna(-1, inplace=True)
            data['bus_sign'] = data['bus_sign'].apply(lambda x: deal_sign(int(x)))
        if 'bus_valid' in data.columns:
            data.bus_valid.fillna(-1, inplace=True)
            data['bus_valid'] = data['bus_valid'].apply(lambda x: deal_valid(int(x)))
        if 'turnover_num' in data.columns:
            data.turnover_num.fillna(-1, inplace=True)
            data['turnover_num'] = data['turnover_num'].apply(lambda x: deal_turnover(int(x)))
        data['score'] = data['lyqdlb'] + data['vip'] + data['bus_allot'] + \
                        data['bus_avgcallduration'] + data['bus_calltimes'] + data['bus_cull_num'] + data['bus_follow_60s'] + \
                        data['bus_followtimes'] + data['bus_invitation_num'] + data['bus_leave'] + data['bus_mark_num'] + \
                        data['bus_sign'] + data['bus_valid'] + data['turnover_num']
        if len(data)==0:
            print("商机没有特征，无法评分")
            data = pd.DataFrame()
            return data
        else:
            # return list(data['score'])[0]
            return data

    def offline_getscore(self):
        try:
            business_scoredf = self.getscore(self.buspre_data)
            print("商机评分分布：\n", business_scoredf["score"].describe())
        except Exception as e :
            print_to_log(e,level=5)
            business_scoredf = pd.DataFrame()
        return business_scoredf


if __name__ == "__main__":
    # rfr_model = BaseModel_RF(p_code=smallyt_develop)
    # rfr_model.rf_train(file_path = data_path)
    # rfr_model.rf_predict(file_path = data_path)
    p_code = smallyt_develop
    division_list = Division().industry_division[p_code]
    train_data = pd.DataFrame(read_bustrain(division_list))
    print(train_data.shape)
    print(train_data.head(5))
