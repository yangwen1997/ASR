#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: serialize_deserialize.py
@time: 2019-05-07 11:34
@desc:序列化和反序列化的工具类 http://www.cnblogs.com/zhangxinqi/p/8034380.html
'''
import pickle


variable = ['hello', 42, [1, 'two'], 'apple']

# 序列化
file_name = 'serial.txt'
file = open(file_name, 'wb')
serialized_obj = pickle.dumps(variable)
file.write(serialized_obj)
file.close()


# 反序列化
target = open(file_name, 'rb')
myObj = pickle.load(target)

print(serialized_obj)
print(myObj)

# 这是一个原生的Python序列化方法。然而近几年来JSON变得流行起来，Python添加了对它的支持。
# 这样更紧凑，而且最重要的是这样与JavaScript和许多其他语言兼容。然而对于复杂的对象，其中的一些信息可能丢失。
import json
encode = json.dumps(variable)
decoded = json.loads(encode)
print('orginal {0} - {1}'.format(variable, type(variable)))
print('encode {0} - {1}'.format(encode, type(encode)))
print('decoded {0} - {1}'.format(decoded, type(decoded)))
