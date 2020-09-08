import os
import sys
sys.path.append(os.environ['PUSHPATH'])
# from center_three.voice_label.remarks_quality.get_remarkdata import Get_Remarkdata
from center_three.voice_label.remarks_quality.get_remarkdata_1day import Get_Remarkdata
from center_three.voice_label.utils import *
import time
import warnings
from datetime import datetime
warnings.filterwarnings("ignore")

def main():
    get_data = Get_Remarkdata()
    inset_mysql = Insert_Into_Mysql()
    try:

        type_code = ""
        regex_data = get_data.get_regex(type_code)
        basic_data = get_data.get_basic_data(time.time())
        if len(basic_data) != 0:
            start = time.time()
            print("我在调用iboss接口")
            post_data = get_data.basic_match_post(basic_data, time.time())
            print("耗时:", time.time()-start)
            if (len(post_data) != 0) & (len(regex_data) != 0):
                match_data = get_data.match_remark(post_data, regex_data)
                if len(match_data) != 0:
                    get_data.mongopool.updata(match_data, MongoDataBase, MongoSet)
                    # label_table_name = insert_table.format(table_data(time.time()))
                    # inset_mysql.flush_hosts(insert_database)
                    # try:
                    #     inset_mysql.update_data_multi(match_data, insert_database, label_table_name)
                    # except Exception as e:
                    #     print("更新质检数据错误", e, get_data.table_data(time.time()))
                    df_all = get_data.get_abnormal(match_data, time.time())
                    if len(df_all) != 0:
                        abnormal_table_name = abnormal_table.format(table_data(time.time()))
                        inset_mysql.flush_hosts(Regex_Data_Base)
                        try:
                            inset_mysql.insert_data_multi(df_all, Regex_Data_Base, abnormal_table_name)
                        except pymysql.ProgrammingError:
                            inset_mysql.create_table(Regex_Data_Base, abnormal_table_name)
                            inset_mysql.insert_data_multi(df_all, Regex_Data_Base, abnormal_table_name)
        else:
            print("没有待质检数据", get_data.table_data(time.time()))
    except Exception as e:
        print(e, get_data.table_data(time.time()))
    finally:
        del get_data
        del inset_mysql

def now_data(timestamp):
    """
    :param timestamp:  时间戳
    :return: 年月 (格式)->2020-07-01 10:10:10
    """
    dt = datetime.fromtimestamp(int(timestamp))
    date = dt.strftime('%Y-%m-%d %H:%M:%S').strip()
    return date

if __name__ == '__main__':
    while 1:
        start = time.time()
        main()
        print("耗时:", time.time() - start, "当前:", now_data(time.time()))
        # d_time0 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '21:05', '%Y-%m-%d%H:%M')
        # d_time1 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '23:59', '%Y-%m-%d%H:%M')

        # d_time2 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '00:00', '%Y-%m-%d%H:%M')
        # d_time3 = datetime.datetime.strptime(str(datetime.datetime.now().date()) + '08:40', '%Y-%m-%d%H:%M')

        # n_time = datetime.datetime.now()
        # if (d_time0 <= n_time and n_time < d_time1) | (d_time2 <= n_time and n_time < d_time3):
        #        time.sleep(60)