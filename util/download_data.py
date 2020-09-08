#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: download.py
@time: 2019-08-30 10:41
@desc:
'''
# coding=utf-8
from flask import Flask, Response, request
import os
import sys

sys.path.append(os.environ['PUSHPATH'])
from loans_drop.config.path import data_path as loans_path
from resource_drop.law_drop import data_path as law_path
from resource_drop.develop_drop import data_path as develop_path
from property.config.path import data_path as property_path
from resource_drop.comprehensive_drop.config.path import data_path as comprehensive_path
from util.mapDGG import data_port
# os.environ["CUDA_VISIBLE_DEVICES"]="-1"


class IbaResponse(Response):
    default_mimetype = 'application/json'


class IbaFlask(Flask):
    response_class = IbaResponse


app = IbaFlask(__name__)


def get_file(file_path=None):
    """
    http://10.0.0.110:22221/property/?filename=online.txt
    """
    try:
        if request.method == 'GET':
            fullfilename = file_path + request.args.get('filename')
            print("*" * 20)
            print(fullfilename)
            print("*" * 20)
            fullfilenamelist = fullfilename.split('/')
            filename = fullfilenamelist[-1]

            #普通下载
            # filepath = fullfilename.replace('/%s' % filename, '')
            # response = flask.make_response(flask.send_from_directory(filepath, filename, as_attachment=True))
            # response.headers["Content-Disposition"] = "attachment; filename={}".format(filepath.encode().decode('latin-1'))
            # return flask.send_from_directory(filepath, filename, as_attachment=True)

            # 流式读取
            def send_file():
                store_path = fullfilename
                with open(store_path, 'rb') as targetfile:
                    while 1:
                        data = targetfile.read(20 * 1024 * 1024)   # 每次读取20M
                        if not data:
                            break
                        yield data

            response = Response(send_file(), content_type='application/octet-stream')
            response.headers["Content-disposition"] = 'attachment; filename=%s' % filename
            return response
    except Exception as e:
        response = Response(str(e), content_type='application/octet-stream')
        response.headers["Content-disposition"] = 'attachment; filename=%s' % filename
        return response


@app.route('/property/', methods=['GET', 'POST'])
def property():
    """
    http://10.0.0.110:22221/property/?filename=online.txt
    """
    return get_file(file_path=property_path)


@app.route('/loans/', methods=['GET', 'POST'])
def loans():
    """
    http://10.0.0.110:22221/loans/?filename=business.csv
    """
    return get_file(file_path=loans_path)


@app.route('/law/', methods=['GET', 'POST'])
def law():
    """
    http://10.0.0.110:22221/law/?filename=business.csv
    """
    return get_file(file_path=law_path)


@app.route('/develop/', methods=['GET', 'POST'])
def develop():
    """
    http://10.0.0.110:22221/develop/?filename=business.csv
    """
    return get_file(file_path=develop_path)


@app.route('/comprehensive/', methods=['GET', 'POST'])
def comprehensive():
    """
    http://10.0.0.110:22221/comprehensive/?filename=business.csv
    """
    return get_file(file_path=comprehensive_path)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=data_port, debug=True)