#!/usr/bin/env python
# encoding: utf-8
'''
@author: sundali
@contact: weichat,sundalisf
@file: tongyong_feature.py
@time: 2019-08-23 13:52
@desc:合并各个模型的通用特征工程
'''

from resource_drop.law_drop.config.feature import continuous_columns, category_columns, all_columns, continuous2category_columns
from resource_drop.loans_drop.feature_engineering.feature_select import *

from resource_drop.property_drop.data_cleaning.decode_field import deal_origin_type, deal_sjlyqd, deal_type_code
from resource_drop.loans_drop.data_cleaning.decode_field import cat_replace, numpy_minmax
from resource_drop.law_drop.data_cleaning.decode_field import deal_follow_times, deal_invalid_number, deal_allot_number, \
    deal_meetingcount, deal_work_age, deal_age, deal_avgworkhour

from resource_drop.develop_drop.data_cleaning.decode_field import splitfeature_part

import datetime


class BaseEngineering(object):

    def __init__(self):
        self.continuations = continuous_columns
        self.categories = category_columns
        self.threshold = np.random.uniform(low=0, high=0.1, size=1)[0]

    def make_report(self, data=None):
        """处理异常值"""
        dic = {}
        for name in self.continuations:
            if name not in data.columns:
                continue
            data[name] = data[name].astype('float')
            sample = data[name].drop(data[data[name] == 0].index)

            sample_max = sample.max()
            dic2 = dict()
            # dic2['去除0后的总人数'] = len(sample)
            try:
                dic2['众数'] = sample.mode()[0]
            except:
                dic2['众数'] = None

            try:
                dic2['中位数'] = sample.median()
            except:
                dic2['中位数'] = None
            ss = sample.describe()
            for i in range(len(ss)):
                dic2[ss.index[i]] = ss[i]

            value = sample.quantile(q=0.75) + sample.std()
            # dic2['建议取值'] = value if dic2['max'] > value else dic2['max']
            dic2['建议取值'] = value if sample_max > value else sample_max
            # print(name, dic2['75%'], dic2['std'], dic2['建议取值'], dic2['max'])
            #         dic2['建议取值'] = dic2['建议取值'].astype(int)
            dic[name] = dic2
        return dic

    def deal(self, data=None, employee_df=None, business_df=None, classifications='binary', data_path=None):

        # TODO:==========================原数据整合===========================
        data["p_date"] = pd.to_datetime(data.p_date)
        p_date = data["p_date"]

        print('检查被正确拿下来的特征')
        self.continuations = []
        self.categories = []
        i = 0
        for name in data.columns:
            if name in continuous_columns:
                self.continuations.append(name)
            elif name in category_columns:
                self.categories.append(name)
            else:
                print(name, '拉取中的数据中不在continuous_columns和category_columns')
                i += 1
        print('dsd', data.columns)
        print("拉取中的数据中不在continuous_columns和category_columns的字段个数为%s" % i)
        print('The remaining features after filter:', len(self.continuations) + len(self.categories))
        print('开始负采样')
                # data = sampling_with_followDays(data=data, classifications=classifications, continuous_columns=self.continuations,category_columns=self.categories)

        if 'business_location' in self.categories:
            self.categories.remove('business_location')
            print('The remaining features after filter:', len(self.continuations) + len(self.categories))

        # 测试(df1['follow_days1'] == 1) | (df1['follow_days1'] == 2)作为负样本是否合适
        #         sample = data[['follow_bins', 'label']]
        #         sample['count'] = 1
        #         sample = sample.groupby(['follow_bins', 'label']).agg({'count': np.sum})
        #         print(sample)

        # TODO: 处理连续型特征
        print('根据业务信息进行分箱:')
        if 'follow_times' in data.columns:
            data.follow_times.fillna(0, inplace=True)
            data['follow_times'] = data['follow_times'].apply(lambda x: deal_follow_times(float(x)))
            if employee_df is not None and business_df is not None:
                if 'follow_times' in employee_df.columns:
                    employee_df.follow_times.fillna(0, inplace=True)
                    employee_df['follow_times'] = employee_df['follow_times'].apply(
                        lambda x: deal_follow_times(float(x)))
                elif 'follow_times' in business_df.columns:
                    business_df.follow_times.fillna(0, inplace=True)
                    business_df['follow_times'] = business_df['follow_times'].apply(
                        lambda x: deal_follow_times(float(x)))

        if 'invalid_number' in data.columns:
            data.invalid_number.fillna(0, inplace=True)
            data['invalid_number'] = data['invalid_number'].apply(lambda x: deal_invalid_number(float(x)))
            if employee_df is not None and business_df is not None:
                if 'invalid_number' in employee_df.columns:
                    employee_df.invalid_number.fillna(0, inplace=True)
                    employee_df['invalid_number'] = employee_df['invalid_number'].apply(
                        lambda x: deal_invalid_number(float(x)))
                elif 'invalid_number' in business_df.columns:
                    business_df.invalid_number.fillna(0, inplace=True)
                    business_df['invalid_number'] = business_df['invalid_number'].apply(
                        lambda x: deal_invalid_number(float(x)))

        if 'allot_number' in data.columns:
            data.allot_number.fillna(0, inplace=True)
            data['allot_number'] = data['allot_number'].apply(lambda x: deal_allot_number(float(x)))
            if employee_df is not None and business_df is not None:
                if 'allot_number' in employee_df.columns:
                    employee_df.allot_number.fillna(0, inplace=True)
                    employee_df['allot_number'] = employee_df['allot_number'].apply(
                        lambda x: deal_allot_number(float(x)))
                elif 'allot_number' in business_df.columns:
                    business_df.allot_number.fillna(0, inplace=True)
                    business_df['allot_number'] = business_df['allot_number'].apply(
                        lambda x: deal_allot_number(float(x)))

        if 'ordercount' in data.columns:
            data['ordercount'] = data['ordercount'].notnull() * 1
            if employee_df is not None and business_df is not None:
                if 'ordercount' in employee_df.columns:
                    employee_df['ordercount'] = employee_df['ordercount'].notnull() * 1
                elif 'ordercount' in business_df.columns:
                    business_df['ordercount'] = business_df['ordercount'].notnull() * 1

        if 'meetingcount' in data.columns:
            data.meetingcount.fillna(0, inplace=True)
            data['meetingcount'] = data['meetingcount'].apply(lambda x: deal_meetingcount(float(x)))
            if employee_df is not None and business_df is not None:
                if 'meetingcount' in employee_df.columns:
                    employee_df.allot_number.fillna(0, inplace=True)
                    employee_df['meetingcount'] = employee_df['meetingcount'].apply(
                        lambda x: deal_meetingcount(float(x)))
                elif 'meetingcount' in business_df.columns:
                    business_df.allot_number.fillna(0, inplace=True)
                    business_df['meetingcount'] = business_df['meetingcount'].apply(
                        lambda x: deal_meetingcount(float(x)))

        if 'work_age' in data.columns:
            data.fillna(0, inplace=True)
            data['work_age'] = data['work_age'].apply(lambda x: deal_work_age(float(x)))
            if employee_df is not None and business_df is not None:
                if 'work_age' in employee_df.columns:
                    employee_df.fillna(0, inplace=True)
                    employee_df['work_age'] = employee_df['work_age'].apply(lambda x: deal_work_age(float(x)))
                elif 'work_age' in business_df.columns:
                    business_df.fillna(0, inplace=True)
                    business_df['work_age'] = business_df['work_age'].apply(lambda x: deal_work_age(float(x)))

        if 'age' in data.columns:
            data.fillna(0, inplace=True)
            data['age'] = data['age'].apply(lambda x: deal_age(float(x)))
            print("data.age.mean(): ", data.age.mean())
            if employee_df is not None and business_df is not None:
                if 'age' in employee_df.columns:
                    employee_df.fillna(0, inplace=True)
                    employee_df['age'] = employee_df['age'].apply(lambda x: deal_age(float(x)))
                elif 'age' in business_df.columns:
                    business_df.fillna(0, inplace=True)
                    business_df['age'] = business_df['age'].apply(lambda x: deal_age(float(x)))

        if 'avgworkhour' in data.columns:
            data.avgworkhour.fillna(0, inplace=True)
            data['avgworkhour'] = data['avgworkhour'].apply(lambda x: deal_avgworkhour(float(x)))
            if employee_df is not None and business_df is not None:
                if 'avgworkhour' in employee_df.columns:
                    employee_df.fillna(0, inplace=True)
                    employee_df['avgworkhour'] = employee_df['avgworkhour'].apply(lambda x: deal_avgworkhour(float(x)))
                elif 'avgworkhour' in business_df.columns:
                    business_df.fillna(0, inplace=True)
                    business_df['avgworkhour'] = business_df['avgworkhour'].apply(lambda x: deal_avgworkhour(float(x)))

        print("开始对连续型变量分箱:", continuous2category_columns)
        data = splitfeature_part(data, continuous2category_columns)
        if employee_df is not None and business_df is not None:
            employee_df = splitfeature_part(employee_df, continuous2category_columns)
            business_df = splitfeature_part(business_df, continuous2category_columns)

        print('利用quartile处理outlier:')
        describe_df = pd.DataFrame(self.make_report(data=data))
        for name in self.continuations:
            limit = float(describe_df.loc['建议取值', name])

            def deal_outlier(x):
                if x > limit:
                    return limit
                elif x < 0:
                    return 0
                else:
                    return x

            if name in data.columns:
                data[name] = data[name].apply(lambda x: deal_outlier(float(x)))

            if employee_df is not None and business_df is not None:
                if name in employee_df.columns:
                    employee_df[name] = employee_df[name].apply(lambda x: deal_outlier(float(x)))
                elif name in business_df.columns:
                    business_df[name] = business_df[name].apply(lambda x: deal_outlier(float(x)))

        print('通过Missing Value筛选特征')
        self.continuations = missing_ratio(data=data[self.continuations], rate=50)
        self.categories = missing_ratio(data=data[self.categories], rate=50)
        print('The remaining features after filter:', len(self.continuations) + len(self.categories))

        # 归一化后进行低方差和高相关滤波
        print('MinMaxScaler')
        for name in self.continuations:
            data[name], min, max = numpy_minmax(X=data[name])
            if employee_df is not None and business_df is not None:
                if name in employee_df.columns:
                    employee_df[name], _, _ = numpy_minmax(X=employee_df[name], xmin=min, xmax=max)
                elif name in business_df.columns:
                    business_df[name], _, _ = numpy_minmax(X=business_df[name], xmin=min, xmax=max)

        print('通过correlation筛选特征')
        remove = correlation(data=data[self.continuations], threshold=0.9)
        for name in remove:
            if name in self.continuations:
                self.continuations.remove(name)
        print('removed: ', remove)
        print('The remaining features after filter:', len(self.continuations) + len(self.categories))

        # TODO:=====================删除缺失值较多的样本===============
        employees = []
        businesses = []
        if employee_df is not None and business_df is not None:
            for name in self.continuations + self.categories:
                print('name', name)
                if name in remove:
                    continue
                elif name in employee_df.columns:
                    employees.append(name)
                elif name in business_df.columns:
                    businesses.append(name)
                else:
                    continue
            print('employee_df before dropNA thresh>4', employee_df.shape)
            print(employee_df.columns)
            print(employees)
            employee_df = employee_df[employees + ['follower_id']].dropna(axis=0, thresh=4, inplace=False)
            print('employee_df after dropNA thresh>4', employee_df.shape)
            print('business_df before dropNA thresh>4', business_df.shape)
            business_df = business_df[businesses + ['business_id']].dropna(axis=0, thresh=4, inplace=False)
            print('business_df after dropNA thresh>4', business_df.shape)

        # TODO:=====================利用字典树替换特征取值===============
        data = cat_replace(data=data)
        if employee_df is not None and business_df is not None:
            employee_df = cat_replace(data=employee_df)
            business_df = cat_replace(data=business_df)

        # TODO:===================填充缺失值==============
        for name in self.continuations:
            data[name].fillna(0, inplace=True)
            if employee_df is not None and business_df is not None:
                if name in employee_df.columns:
                    employee_df[name].fillna(0, inplace=True)
                elif name in business_df.columns:
                    business_df[name].fillna(0, inplace=True)
            print(data[name].describe())

        for name in self.categories:
            data[name].astype(object).fillna('已经缺失', inplace=True)
            if employee_df is not None and business_df is not None:
                if name in employee_df.columns:
                    employee_df[name].astype(object).fillna('已经缺失', inplace=True)
                elif name in business_df.columns:
                    business_df[name].astype(object).fillna('已经缺失', inplace=True)

        if "allot_number" in data.columns:
            data.loc[data['allot_number'].astype(int) > 3, 'allot_number'] = 4
            if employee_df is not None and business_df is not None:
                if 'allot_number' in employee_df.columns:
                    employee_df.loc[employee_df['allot_number'].astype(int) > 3, 'allot_number'] = 4
                elif 'allot_number' in business_df.columns:
                    business_df.loc[business_df['allot_number'].astype(int) > 3, 'allot_number'] = 4

        if 'origin_code' in data.columns:
            data['origin_code'].replace(deal_sjlyqd(data=data), inplace=True)
            # data = data[~data['originCode'].isin(['已禁用'])]
            # print(data['originCode'].value_counts())
            if employee_df is not None and business_df is not None:
                if 'origin_code' in employee_df.columns:
                    employee_df['origin_code'].replace(deal_sjlyqd(data=employee_df), inplace=True)
                elif 'origin_code' in business_df.columns:
                    business_df['origin_code'].replace(deal_sjlyqd(data=business_df), inplace=True)

        if 'origin_type' in data.columns:
            data['origin_type'].replace(deal_origin_type(data=data), inplace=True)
            if employee_df is not None and business_df is not None:
                if 'origin_type' in employee_df.columns:
                    employee_df['origin_type'].replace(deal_origin_type(data=employee_df), inplace=True)
                elif 'origin_type' in business_df.columns:
                    business_df['origin_type'].replace(deal_origin_type(data=business_df), inplace=True)

        if 'type_code' in data.columns:
            data['type_code'].replace(deal_type_code(data=data), inplace=True)
            if employee_df is not None and business_df is not None:
                if 'type_code' in employee_df.columns:
                    employee_df['type_code'].replace(deal_type_code(data=employee_df), inplace=True)
                elif 'type_code' in business_df.columns:
                    business_df['type_code'].replace(deal_type_code(data=business_df), inplace=True)

        self.categories = concentration_ratio(data[self.categories])
        print('The remaining features after filter:', len(self.continuations) + len(self.categories))

        print('dssc', data.columns)
        #         data = data[self.continuations + self.categories + ['label', 'login_name', 'customer_id']]
        data = data[self.continuations + self.categories + ["label", "follower_id", "business_id", ]]

        print("===================离散特征阿拉伯数字编码==============")
        for name in self.categories:
            print(data[name].value_counts())
            values = list(set(data[name].values.tolist()))
            value_map = {values[i]: i + 1 for i in range(len(values))}
            # print(values)
            data[name] = data[name].map(value_map)
            print(data[name].value_counts())
            print()
            if employee_df is not None and business_df is not None:
                if name in employee_df.columns:
                    employee_df[name] = employee_df[name].map(value_map)
                elif name in business_df.columns:
                    business_df[name] = business_df[name].map(value_map)

        print('The remaining features after filter:', len(self.continuations) + len(self.categories))
        print(data['label'].value_counts())

        # TODO:===================缓存商务、商机特征数据==============
        employees = []
        businesses = []
        if employee_df is not None and business_df is not None:
            for name in self.continuations + self.categories:
                if name in employee_df.columns:
                    employees.append(name)
                elif name in business_df.columns:
                    businesses.append(name)
                else:
                    print('error data的特征%s既不属于employee_df, 也不属于business_df' % name)

            employee_df = employee_df[employees + ["follower_id"]]
            employee_df["follower_id"] = employee_df["follower_id"].astype(str)
            business_df = business_df[businesses + ["business_id"]]
            business_df["business_id"] = business_df["business_id"].astype(int)
            # employee_df.set_index("follower_id",inplace=True)   #为了lgb/xgb模块中的predict中的获取商务信息速度更快
            # business_df.set_index("business_id",inplace=True)

            employee_path = data_path + 'employee{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
            business_path = data_path + 'business{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))

            # employee_df.to_csv(employee_path, encoding='utf-8',sep=',')
            # business_df.to_csv(business_path, encoding='utf-8',sep=',')  #employee_id作为index，to_csv不管index为0、1都是一样的结果。只是read_csv要index_col=0
            employee_df.to_csv(employee_path, encoding='utf-8', sep=',', index=0)
            business_df.to_csv(business_path, encoding='utf-8', sep=',', index=0)
            print("employee_path :", employee_path)
            print("business_path :", business_path)

            # employee_df.to_csv(employee_path, encoding='utf-8', index=0, sep=',')
            # business_df.to_csv(business_path, encoding='utf-8', index=0, sep=',')
            print('特征工程后的商务样本:', employee_df.shape)
            print('特征工程后的客户样本:', business_df.shape)

        # TODO:===================添加p_date方便进行训练集测试集切分===========
        data["p_date"] = p_date
        data["p_date_month"] = data["p_date"].dt.month
        data_month = data["p_date_month"].unique()
        print("total_data中数据的月份为%s" % (data_month))
        print("训练数据的各月份正负样本数量为：")
        #  查看各个月份的各业态正负样本比例，3，4月份全取的正样本
        p_date1 = data[["p_date_month", "label"]].groupby(["p_date_month"], as_index=False).count()
        p_date1.columns = ["p_date_month", "总计"]
        p_date2 = data[["p_date_month", "label"]].groupby(["p_date_month"], as_index=False).sum()
        p_date2.columns = ["p_date_month", "正样本数量"]
        p_date3 = p_date2.merge(p_date1)
        p_date3["负样本数量"] = (p_date3["总计"]).astype('float') - (p_date3["正样本数量"]).astype('float')
        print(p_date3)

        # 查看数据时间
        print("最早的样本时间为%s" % data.p_date.min())
        print("最晚的样本时间为%s" % data.p_date.max())
        self.drop = data.drop("p_date_month", axis=1, inplace=True)
        data_path = data_path + 'data{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d'))
        print("data_path data{}.csv: ", data_path)
        data.to_csv(data_path, encoding='utf-8', index=0, sep=',')
        print(str(self), "End")
        return data


if __name__ == '__main__':
    # 1. 读取融资数据并添加列名
    from loans_drop.config.path import data_path

    df = pd.read_csv(data_path + "total_data.csv", sep="", header=None)
    df.replace("\\N", np.NaN, inplace=True)
    df.columns = all_columns
    #     print(df["p_code"].unique())
    # df = df.loc[df.p_code == "BUS_YT_DK"]  # 融资
    #     df = df.loc[df.p_code=="BUS_YT_CY"] #创发
    # df = df.loc[df.p_code=="BUS_YT_FL"]
    #     data = data.loc[(data.p_code=="BUS_YT_DK")|(data.p_code=="BUS_YT_FL")|(data.p_code=="BUS_YT_CY")]

    # 2. 变换p_date并保存
    # df["p_date"] = pd.to_datetime(df.p_date)
    # p_date = df["p_date"]
    # null_rate, feature_list = null_statistic(df)
    # print(null_rate)
    #     df = df.loc[:,category_columns1+continuous_columns1+["label"]]
    print(df['label'].value_counts())
    print(df.shape)
    print(df.info())

    # 3. 进行特征工程处理
    df = BaseEngineering().deal(data=df)
    model_columns = df.columns
    # 4. 添加p_date，方便进行训练集测试集切分
    # df["p_date"] = p_date
    print(df.shape)
