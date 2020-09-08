#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: base_model.py
@time: 2019-08-07 11:46
@desc: 用于实现各个模型所需要的编程规范
'''
from base_class.feature import BaseEngineering
from base_class.read_mongo import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from util.c_define import *
from util.read_database.map_tree import Division
from resource_drop.property_drop.data_cleaning.sampling import sampling_with_followDays as sample_property
from resource_drop.property_drop.config.feature import continuous_columns as continuous_property
from resource_drop.property_drop.config.feature import category_columns as category_property
from resource_drop.property_drop.data_cleaning.sampling import sampling_with_followDays as sample_comprehensive
from resource_drop.comprehensive_drop.config.feature import continuous_columns as continuous_comprehensive
from resource_drop.comprehensive_drop.config.feature import category_columns as category_comprehensive


class BaseModel(object):
    """
    制定一些所有业态都需要遵守的规范，具体实现由各个子类负责
    """

    def __init__(self, p_code=None):
        print("开始初始化model: ", str(self))
        self.X = None
        self.model = None
        self.classifications = 'binary'
        self.employee_data = None
        self.business_data = None
        if p_code is not None:
            self.business_p_code = p_code
        else:
            raise NotImplementedError("self.business_p_code必须在初始化时赋值, 用于标记模型的业态")

        try:
            # 单机部署
            self.train()
        except:
            print_to_log("模型训练出错", level=5)

        # if current_smallyt == smallyt_loans:
        #     self.train()
        self.desc = 'This is BaseModel, please overwrite this property'

    def load_data(self, file_path=None, engineer=BaseEngineering()):
        if file_path is None:
            base_dir = os.getcwd()
            print(base_dir)
            # d = path.dirname(__file__)  #返回当前文件所在的目录
            # abspath = path.abspath(d) #返回d所在目录规范的绝对路径
            # 数据文件路径
            file_path = os.path.abspath(os.path.dirname(base_dir) + os.path.sep + ".") + '/data/'
            print_to_log("file_path is None, 设置默认值：", file_path)

        print(str(self))
        p_code = self.business_p_code
        employee_path = file_path + 'employee{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        business_path = file_path + 'business{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        train_data_path = file_path + 'data{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        employee_origin_path = file_path + 'employee_origin{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        business_origin_path = file_path + 'business_origin{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        train_origin_path = file_path + 'train{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        is_exists_train = os.path.exists(train_data_path)
        is_exists_employee = os.path.exists(employee_path)
        is_exists_business = os.path.exists(business_path)
        is_origin_train = os.path.exists(train_origin_path)
        is_origin_employee = os.path.exists(employee_origin_path)
        is_origin_business = os.path.exists(business_origin_path)

        try:
            print('Division().industry_division: ', Division().industry_division)
            division_list = Division().industry_division[p_code]
        except Exception as e:
            division_list = []
            print_to_log("事业部id获取失败, 通过商机业态读取train_data", level=5)

        if len(division_list) > 0:
            print("===============================读取训练数据===============================")
            if p_code == 'BUS_YT_DK':
                df = pd.DataFrame(read_push(p_code='BUS'))
                df.to_csv(train_origin_path, encoding='utf-8', sep=',', index=0)
            else:
                if is_origin_train:
                    df = pd.read_csv(train_origin_path)
                else:
                    df = pd.DataFrame(read_train_with_division(division_ids=division_list))
                    df.to_csv(train_origin_path, encoding='utf-8', sep=',', index=0)
                print(df.shape, df.columns)
            print("===============================读取商机数据===============================")
            if is_origin_business:
                df1 = pd.read_csv(business_origin_path)
            else:
                df1 = pd.DataFrame(read_business_with_division(division_ids=division_list))
                df1.to_csv(business_origin_path, encoding='utf-8', sep=',', index=0)
            print(df1.shape)
        else:
            print("===============================读取训练数据===============================")
            if is_origin_train:
                df = pd.read_csv(train_origin_path)
            else:
                df = pd.DataFrame(read_train(p_code=p_code))
                df.to_csv(train_origin_path, encoding='utf-8', sep=',', index=0)
            print(df.shape)
            print("===============================读取商机数据====================================")
            if is_origin_business:
                df1 = pd.read_csv(business_origin_path)
            else:
                df1 = pd.DataFrame(read_business(p_code=p_code))
                df1.to_csv(business_origin_path, encoding='utf-8', sep=',', index=0)
            print(df1.shape)

        print("===============================读取商务数据===============================")
        if is_origin_employee:
            df2 = pd.read_csv(employee_origin_path)
        else:
            # df2 = pd.DataFrame(read_employee(p_code=p_code))
            #df2 = pd.DataFrame(read_emp(division_ids=division_list))
            df2 = pd.DataFrame(read_emp())
            df2['follower_id'] = df2['follower_id'].astype('int64')
            df2.to_csv(employee_origin_path, encoding='utf-8', sep=',', index=0)
        print(df2.shape)

        print_to_log("Start sample", df.shape, df1.shape, df2.shape)
        if self.business_p_code == smallyt_property:
            df = sample_property(k=20, data=df, classifications=self.classifications,
                                 continuous_columns=continuous_property, category_columns=category_property)
        elif self.business_p_code == smallyt_comprehensive:
            df = sample_comprehensive(k=20, data=df,
                                      classifications=self.classifications,
                                      continuous_columns=continuous_comprehensive,
                                      category_columns=category_comprehensive)

        if is_exists_train and is_exists_employee and is_exists_business:
            df = pd.read_csv(train_data_path)
        else:
            # 如果要做负采样，必须在特征工程之前
            print_to_log("Start Engineering", df.shape, df1.shape, df2.shape)
            df = engineer.deal(data=df, employee_df=df2, business_df=df1,
                               classifications=self.classifications, data_path=file_path)
            print_to_log("End Engineering")
        print_to_log("employee_path : ", employee_path)
        print_to_log("business_path : ", business_path)
        self.employee_data = pd.read_csv(employee_path)
        self.business_data = pd.read_csv(business_path)
        self.business_ids = list(self.business_data['business_id'])
        print_to_log("load_data end")
        return df

    def update_data(self):
        raise NotImplementedError("为了实现所有业态的编程规范, 该方法必须实现")

    def train(self, data=None):
        if Division().industry_division is None:
            raise NotImplementedError("iboss业态编码错误率高，所以self.industry_division在train之前必须被赋值, 用于predict时查询商机特征")
        raise NotImplementedError("为了实现所有业态的编程规范, 该方法必须实现")

    def predict(self, bus_id=None, employee_df=None):
        if Division().industry_division is None:
            raise NotImplementedError("iboss业态编码错误率高，所以self.industry_division在train之前必须被赋值, 用于predict时查询商机特征")
        raise NotImplementedError("为了实现所有业态的编程规范, 该方法必须实现")

    def verification(self, employee_ids=None, business_ids=None):
        if len(employee_ids) != len(business_ids):
            return None
        for i in range(len(business_ids)):
            employee = employee_ids[i]
            business = business_ids[i]
            employees_data = self.employee_data[self.employee_data[employee_key] == employee]
            if "userId" in employees_data.columns:
                employees_data.drop(["userId"], axis=1, inplace=True)
            if "Unnamed: 0" in employees_data.columns:
                employees_data.drop(["Unnamed: 0"], axis=1, inplace=True)
            self.predict(bus_id=business, employee_df=employees_data)

    def update_model(self):
        print('如果子类未使用单例模式，则为了实现所有业态的编程规范，必须重写此方法')

    def deal_cols_order(self, prediction_data):
        """数据特征顺序与训练集保持一致,
        :arg prediction_data 带预测数据
        :return 处理完顺序的数据框
        """
        try:
            prediction_data = prediction_data[self.X.columns]
        except:
            for name in self.X.columns:
                if name not in prediction_data.columns:
                    prediction_data[name] = 0
            prediction_data = prediction_data[self.X.columns]
        temp_df = pd.DataFrame([prediction_data.columns, self.X.columns]).T
        print("训练数据与预测数据列名中顺序不一致的个数为%s" % (temp_df[0] != temp_df[1]).sum())
        print(prediction_data.columns)
        return prediction_data

    def feature_evaluation(self, need_plot=False):
        model_name = str(self)
        print_to_log("feature_evaluation:")
        if "LGB" in model_name:
            dataframe = pd.DataFrame(self.X.columns.tolist(), columns=['feature'])
            dataframe['importance'] = list(self.model.feature_importance())
            dataframe = dataframe.sort_values(by='importance', ascending=False)
            print(dataframe)
            if need_plot:
                plt.figure(figsize=(180, 90))
                import lightgbm as lgb
                lgb.plot_importance(self.model, max_num_features=40)
                plt.title("Featurertances")
                plt.show()
        elif "XGB" in model_name:
            dataframe = pd.DataFrame(self.X.columns.tolist(), columns=['feature'])
            print("self.model.feature_importances_:", self.model.feature_importances_)
            dataframe['importance'] = list(self.model.feature_importances_)
            dataframe = dataframe.sort_values(by='importance', ascending=False)
            print(dataframe)
            if need_plot:
                plt.figure(figsize=(180, 90))
                from xgboost import plot_importance
                plot_importance(self.model)
                plt.title("Featurertances")
                plt.show()
        else:
            print("其他模型自行实现")

    def get_business_df(self, business_id=None):
        print(str(self))
        """customer_id int类型， 查找商机特征数据"""
        self.business_data[business_key] = self.business_data[business_key].astype(int)
        return self.business_data[self.business_data[business_key] == int(business_id)].drop_duplicates(business_key,
                                                                                                        keep='first')

    def get_prediction_data(self, business_id=None, employee_df=None):
        if "follower_id" in employee_df.columns:
            employee_df.drop('follower_id', axis=1, inplace=True)
        print_to_log('待选商务列表:', employee_df.shape)
        business_df = self.get_business_df(business_id=business_id)
        result = dict()
        # 1. 如果找不到商机
        if len(business_df) == 0:
            result['user'] = None
            result['desc'] = '商机%s没有特征，概率太低,不予推送' % business_id
            result['probability'] = 0
            return result

        print('business_df', business_df.columns)
        print('employee_df', employee_df.columns)
        business_df = pd.concat([business_df] * len(employee_df), ignore_index=True).reset_index(drop=True)
        print('business_df', business_df.columns)
        print('business_df After 重采样:', business_df.shape)

        if len(employee_df) == len(business_df):
            print('prediction_data Before concat:', employee_df.shape, business_df.shape)
            business_df.index = range(len(business_df))
            employee_df.index = range(len(employee_df))
            prediction_data = pd.concat((employee_df, business_df), axis=1)
            print('prediction_data After concat:', prediction_data.shape)
            print(prediction_data.head())
            if "business_id" in prediction_data:
                prediction_data.drop('business_id', axis=1, inplace=True)

            prediction_data = self.deal_cols_order(prediction_data)
            return prediction_data

        else:
            return None

    def get_result_from_xgb(self, preds=None, employee_list=None, weight_list=None):
        result = dict()
        try:
            std_value = np.std(preds)
            print_to_log('方差:', np.var(preds))
            print_to_log('标准差:', std_value)

            # 二分类任务
            if self.classifications == 'binary':
                index = np.argsort(preds)[::-1]
                print_to_log('index:', index)
                high_employee = []
                high_employee_weight = []
                for i in range(len(preds)):
                    if preds[i] > 0.4:
                        high_employee.append(employee_list[i])
                        high_employee_weight.append(weight_list[i])

                # 没有使用方差随机 考虑到model的泛化能力弱和商务饱和度
                # high_value = len(max_probabilitys) if len(max_probabilitys) > 0 else len(preds)
                # random_index = np.random.randint(low=0, high=high_value, size=1)[0]
                if len(high_employee) > 0:
                    pre_employee = Division().choice_employee(data=high_employee, weight=high_employee_weight)
                else:
                    # random_index = Division().choice_employee(data=index, weight=[weight_list[inx] for inx in index])
                    # pre_employee = employee_list[index[random_index]]
                    pre_employee = Division().choice_employee(data=employee_list, weight=weight_list)
                pre_probability = preds[employee_list.index(pre_employee)]

                result['user'] = pre_employee
                if len(high_employee) > 0:
                    result['desc'] = '预测成功,概率为%s，从大于0.4的商务中进行随机预测' % (str(pre_probability))
                else:
                    result['desc'] = '预测成功,概率为%s，从所有商务中随机选取' % (str(pre_probability))
                result['probability'] = pre_probability
            else:
                # 多分类任务
                pred_label = [np.argmax(pred_list) for pred_list in preds]
                pred_probability = [pred_list[np.argmax(pred_list)] for pred_list in preds]
                print_to_log('pred_label:', pred_label)
                print_to_log('pred_probability:', pred_probability)

                def get_result_by_label(max_label=0):
                    max_probability = 0
                    result_dic = dict()
                    for i in range(len(pred_label)):
                        label = pred_label[i]
                        if label == max_label and pred_probability[i] > max_probability:
                            max_probability = pred_probability[i]
                            result_dic['user'] = employee_list[i]
                            result_dic['desc'] = '预测成功,类别为%d,概率为%s' % (max_label, str(max_probability))
                            result_dic['probability'] = max_probability
                    return result_dic

                if len(pred_label) > 0:
                    value = pred_label[np.argmax(pred_label)]
                    if value == 2:
                        result = get_result_by_label(max_label=2)
                    elif value == 1:
                        result = get_result_by_label(max_label=1)
                    else:
                        # 预测为不能签单，不进行推荐商务
                        result = get_result_by_label(max_label=0)
                        result['user'] = None

        except Exception as e:
            result['user'] = None
            result['desc'] = 'error:组装dictionary出错'
            result['probability'] = 0
            print_to_log(e, level=4)
        return result

    def predict_xgb(self, bus_id=None, employee_df=None):
        # 这一小块抽不到其他方法里面去，follower_id在后面的代码中会被删除，必须先保存
        employee_list = employee_df['follower_id'].values.tolist()
        print('拥有特征数据的employee_list:', employee_list)
        weight_list = employee_df[residual_number].values.tolist()
        print('employee_list对应的权重数据weight_list:', weight_list)

        prediction_data = self.get_prediction_data(business_id=bus_id, employee_df=employee_df)
        if isinstance(prediction_data, dict):
            return prediction_data
        elif prediction_data is not None:
            # print(self.model.predict(prediction_data))
            predict_proba = self.model.predict_proba(prediction_data)[:, 1]
            print_to_log('预测结果:', predict_proba)
            result = self.get_result_from_xgb(preds=predict_proba, employee_list=employee_list, weight_list=weight_list)
        else:
            result = dict()
            result['user'] = None
            result['desc'] = '商务和商机数据长度不匹配，无法关联'
            result['probability'] = 0

        print("result: ", result)
        return result

    def predict_lgb(self, bus_id=None, employee_df=None):
        # 这一小块抽不到其他方法里面去，follower_id在后面的代码中会被删除，必须先保存
        employee_list = employee_df['follower_id'].values.tolist()
        print('拥有特征数据的employee_list:', employee_list)
        weight_list = employee_df[residual_number].values.tolist()
        print('employee_list对应的权重数据weight_list:', weight_list)

        prediction_data = self.get_prediction_data(business_id=bus_id, employee_df=employee_df)
        result = dict()
        if isinstance(prediction_data, dict):
            return prediction_data
        elif prediction_data is not None:
            predict_proba = self.model.predict(prediction_data, num_iteration=self.model.best_iteration)
            print_to_log('预测结果:', predict_proba)
            result = self.get_result_from_xgb(preds=predict_proba, employee_list=employee_list, weight_list=weight_list)
        else:
            result['user'] = None
            result['desc'] = '商务和商机数据长度不匹配，无法关联'
            result['probability'] = 0

        print(result)
        return result

    def predict_vertify(self, business_id_list, follower_id_list, random_num=None):
        """利用已有模型对已成单商机进行成单概率预测"""

        # def save_data(temp_df, df_name):
        #     """保存数据的函数"""
        #     if "LGB" in str(base_model):
        #         temp_df.to_excel(data_path + df_name + "LGB_vertify.xlsx", index=0)
        #     elif "XGB" in str(base_model):
        #         temp_df.to_excel(data_path + df_name + "XGB_vertify.xlsx", index=0)
        #     else:
        #         temp_df.to_excel(data_path + df_name + "Model_chengdan_vertify.xlsx", index=0)
        #     print("数据保存完毕")
        #     return None
        def predict_probability(business_id_list, follower_id_list):
            """内部函数，方便随机预测的调用"""
            probabilities = []
            # 1. 对每条记录进行概率预测
            for bus_id, follow_id in zip(business_id_list, follower_id_list):
                try:
                    followers = self.employee_data[
                        self.employee_data.follower_id == int(follow_id)]  # 根据类属性提取对应商务id和商机id的信息
                    followers.index = range(len(followers))
                    businessses = self.business_data[self.business_data.business_id == int(bus_id)]
                    businessses.index = range(len(businessses))
                    # print()
                    # print("商务信息df长度：%s" % len(followers))
                    # print("商机信息df长度：%s" % len(businessses))
                    if (len(followers) == 0) | (len(businessses) == 0):
                        if (len(followers) == 0) & (len(businessses) != 0):
                            probabilities.append("找不到商务")
                        elif (len(businessses) == 0) & (len(followers) != 0):
                            probabilities.append("找不到商机")
                        else:
                            probabilities.append("商务商机都找不到")
                    else:
                        # 方式1. 模型predict
                        # prediction_data = pd.concat([followers, businessses], axis=1)
                        # print("预测数据长度为：%s"%prediction_data.shape[0])
                        # prediction_data = prediction_data.drop(["follower_id", "business_id"], axis=1)
                        # prediction_data = self.detail_cols_order(prediction_data)
                        # if "LGB" in str(self):
                        #     probabilities.append((self.model.predict(prediction_data))[0])
                        # if "XGB" in str(self):
                        #     probabilities.append((self.model.predict_proba(prediction_data))[:, 1][0])

                        # 方式2. 类方法predict
                        if "LGB" in str(self):
                            probabilities.append(
                                (self.predict(bus_id=int(bus_id), employee_df=followers))["probability"])
                        if "XGB" in str(self):
                            probabilities.append(
                                (self.predict(bus_id=int(bus_id), employee_df=followers))["probability"])
                except Exception as e:
                    probabilities.append("预测数据加载错误" + str(e))
            return probabilities

        # 2.2 对随机组合进行预测
        def predict_random(random_num=None):
            """利用模型对随机组合进行预测的函数"""
            try:
                if isinstance(random_num, int):
                    followers = self.employee_data.sample(random_num, replace=True)
                    followers.index = range(len(followers))
                    businesses = self.business_data.sample(random_num)
                    businesses.index = range(len(businesses))
                    prediction_df = pd.concat([followers, businesses], axis=1)
                    prediction_df = prediction_df.drop(["follower_id", "business_id"], axis=1)
                    prediction_df = self.detail_cols_order(prediction_df)
                    if "LGB" in str(self):
                        random_probabilities = self.model.predict(prediction_df)
                    if "XGB" in str(self):
                        # probabilities.append((self.model.predict_proba(prediction_data))[:, 1][0])
                        random_probabilities = self.model.predict_proba(prediction_df)[:, 1]
                    print_to_log("随机%s个样本的预测概率describe为：" % random_num)
                    temp_df1 = pd.DataFrame(data=[random_probabilities]).T
                    print_to_log(temp_df1[0].describe())
                else:
                    pass
            except Exception as e:
                print_to_log("随机组合数据预测出错：", e)

        # 2. 预测结果展示
        # 2.1 对输入business_id,employee_id进行预测
        predict_random(random_num)
        probabilities = predict_probability(business_id_list=business_id_list, follower_id_list=follower_id_list)
        temp_df = pd.DataFrame([business_id_list, follower_id_list, probabilities]).T
        temp_df.columns = ["business_id", "customer_id", "probability"]
        print("=" * 70)
        print_to_log("概率预测的describe如下")
        print(temp_df["probability"].describe())
        print_to_log("对输入business_id：%s,输入employee_id:%s的预测结果如下：" % (business_id_list, follower_id_list))
        # save_data(temp_df, df_name)
        return temp_df


if __name__ == "__main__":
    base_model = BaseModel()
    base_model.load_data()
