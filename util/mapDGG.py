"""
匹配字典，如果用常量而不是类似这种map = {v: k for k, v in old_map.items()},那就保存在这里
"""
import os


# TODO:当前运行环境
environment = 'DEBUG' # debug环境
# environment = 'RELEASE' # release环境
# environment = 'LOCAL' # 本地环境

debug_speed = True if environment == "DEBUG" else False # 只能改 True if  或者 False if  其它不能动
print("environment", environment, "debug_speed", debug_speed)

# TODO: 智能坐席--张超传送过来的key
# 文本
PhoneText = "text"
# pid
PhonePID = "pid"
# unique_id
PhoneUniqueID = "unique_id"
# 业态编码
PhoneYTCode = "yt_code"
# 分贝
PhoneDecibel = "decibel"
# 语速
PhoneSpeechSpeed = "speech_speed"
# 通话时长
PhoneCallDuration = "call_duration"
# 通话状态
PhoneState = "state"
# 创建时间
PhoneCreateTime = "time"
# 说话方 客户还是商务
PhoneSessionRole = "session_role"
# 话术模板id
PhoneSceneID = "scene_id"

# TODO: 智能坐席--我们自己需要记录的key
# 当前消息是第几个session,如果是放进list 就不需要这个东西
PhoneSessionNumber = "session_number"

# # TODO: 各个业态的标识符
# smallyt_loans = 'BUS_YT_DK'
# smallyt_law = 'BUS_YT_FL'
# smallyt_develop = 'BUS_YT_CY'
# smallyt_property = "BUS_YT_ZSCQ"
# smallyt_comprehensive = "BUS_YT_ZH"
#
# employee_key = "follower_id"
# business_key = "business_id"
#
# # TODO:当前机器部署的业态
#
# current_smallyt = smallyt_property


# TODO:kafka里面传过来的key
# 事业部id
# divisionId = 'divisionId'
# # "areaCode":"BUS_SOR_PLACE_CD"
# areaCode = 'areaCode'
# # "businessId":7807165398043648000
# kafka_businessId = 'businessId'
# # "businessType":"BUS_YT_DK"
# businessType = 'businessType'
# # "createTime":1557477888000
# kafka_createTime = 'createTime'
# # "id":4154,
# kafka_id = 'id'
# # "noteDesc":"",
# noteDesc = 'noteDesc'
# # "orderStatus":"UN_FLOW",
# orderStatus = 'orderStatus'
# # "resAllocat":false,
# resAllocat = 'resAllocat'
# # "resSign":0,
# resSign = 'resSign'
# # "resSuccess":0
# resSuccess = 'resSuccess'
# # "typeAllocat":"B",
# typeAllocat = 'typeAllocat'
# # “userId”:”分配结果需组装，分配到的商务Id”,
# kafka_userId = 'userId'
# # “userNo”:”分配结果需组装，分配到的商务No”,
# kafka_userNo = 'userNo'
# # “alloctTime”:”分配时间戳，分配结果需组装”
# alloctTime = 'alloctTime'
# # 客户评分
# cusScore = "cusScore"

# TODO:Redis里面存储的key
# redis_businessType = 'businessType'
# redis_deptId = 'deptId'
# onlineTime = 'onlineTime'
# redis_userNo = 'userNo'
# redis_userId = 'userId'
# lastBusId = 'lastBusId'
# redis_deptName = 'deptName'
# # 机器学习当天允许的分配数量
# allowAllotB = ""
# # 机器学习当天实际分配的数量
# allottedCountB = ""

# TODO:MySQL中存储的今天可进行分配的商务
# mysql_userId = "id"
# mysql_userNo = "user_no"
# mysql_name = "user_name"
# mysql_level = "user_level"
# mysql_score = "user_score"
# mysql_area = "area_code"
# mysql_pcode = "yt_code"
# mysql_division = "division_id"
# next_allow_allot_value = "next_allow_allot_value" #'商务当天能够分配的资源总数',
# current_allot_count = "current_allot_count" #'商务当天成功分配商机总数',
# followed_count = "followed_count" # '商务上一个工作日商机跟进数量',
# allow_allot_a = "allow_allot_a" #'经验规则当天允许分配的数量',
# allotted_count_a = "allotted_count_a" # '经验规则当天实际分配的数量',
# allow_allot_b = "allow_allot_b" # '机器学习当天允许的分配数量',
# allotted_count_b = "allotted_count_b" #'机器学习当天实际分配的数量',
# can_allot = "can_allot" #'商务当天是否可推送',
# residual_number = "residual" #非mysql字段，自己进行计算

# TODO:kafka通知模型更新的key
# update_model = "update_model"
# update_model = "-1"


log_path = os.environ['PUSHPATH'] + '/log/'
doc_path = os.environ['PUSHPATH'] + '/doc/'

# TODO:接口功能	port
# 调库日志
log_port = 22220
data_port = 22221
model_port = 22222
cross_industry_port = 22223
ocr_port = 22224
chatbot_OA = 22225
similar_question_port = 22226
download_audio = 22227
resource_cross_port = 22228
update_data_port = 22229
ner_port = 22230
chatbot_port = 22231
test_port = 22219 # 测试部门用的端口
intent_port = 22232


# TODO:Tencent词向量获取的方式，model或mongodb
# Word2Vec_Source = "self_word2vec"
Word2Vec_Source = "TencnetModel"
# TencentWV_Source = "Mongodb" # 爬虫mongodb:172.16.74.3：17017，Chatbot.tencent_wordvector


