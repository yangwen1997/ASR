#!/usr/bin/env python
# encoding: utf-8
'''
@author: 罗成
@contact: luocheng@dgg.net
@file: download.py
@time: 2019-08-29 10:27
@desc:
'''


# coding=utf-8
import flask
from flask import Flask, Response, request
import os
import sys
sys.path.append(os.environ['PUSHPATH'])
from util.mapDGG import log_path, log_port
# os.environ["CUDA_VISIBLE_DEVICES"]="-1"


class IbaResponse(Response):
    default_mimetype = 'application/json'


class IbaFlask(Flask):
    response_class = IbaResponse


app = IbaFlask(__name__)


@app.route('/downloadfile/', methods=['GET', 'POST'])
def downloadfile():
    """
    http://10.0.0.110:22220/downloadfile/?filename=online.txt
    """
    try:
        if request.method == 'GET':
            fullfilename = log_path + request.args.get('filename')
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


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=log_port, debug=True)