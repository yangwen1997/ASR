# coding=utf-8
import re
import json
import traceback
from math import *
from dmp.store.db import default_sql as db
from dmp.algor.machine_learning import exec_algor
from util.log import logger
from util.indexFunc import *


def callcode(expr, method_param):
    logger.debug('callcode: {0}'.format(expr))
    expr = parse_express(expr)
    ids = re.findall('#.*?#', expr)
    if ids:
        for index_id in ids:
            # 判断指标中是否带有参数 (#b0f4982772d98b64a997e51190646766|{'Time_granularity':'year','period':3}#)
            if index_id.find('|') > 0:  # 带参数
                id, params_info = index_id.split('#')[1].split('|')
            else:
                id, params_info = index_id.split('#')[1], None
            sqlstr = 'select method, srcid from r_indexs_method where id = %s'
            result = db.query(sqlstr, [id])
            params = method_param[id] if id in method_param else {}
            if result:
                param_str = json.dumps(params.update(json.loads(params_info)),
                                       ensure_ascii=False) if params_info else json.dumps(
                    params, ensure_ascii=False)
                method = re.match(r'^(.*?)://(.*)$', result[0][0])
                db_id = result[0][1]
                if method:
                    method_type, method_name = method.group(1, 2)
                    method_type = method_type.lower()
                    if method_type == 'code':
                        if method_name.find('.'):
                            module_name = '.'.join(method_name.split('.')[0:-1])
                            exec ('import {0}'.format(module_name))
                        expr = expr.replace(index_id, '{0}(**{1})'.format(method_name, param_str))
                    elif method_type == 'sql':
                        expr = expr.replace(index_id, 'pro_sql_method("{0}", {1}, {2})'.format(method_name, param_str, db_id))
    logger.debug('parse result: {0}'.format(expr))
    try:
        return eval(expr)
    except Exception as ex:
        logger.error(traceback.format_exc())
        return None


def parse_express(expr):
    ids = re.findall('\$.*?\$', expr)
    if ids:
        # 替换自定义公式
        for index_id in ids:
            sqlstr = 'select expression, index_type from r_indexes_info where id = %s'
            iid = index_id.split('$')[1]
            result = db.query(sqlstr, [iid])
            if result:
                index_type = result[0][1]
                expression = result[0][0]
                if index_type == 1:  # and (expression is None or len(expression) < 1):
                    expression = iid
                    expr = expr.replace(index_id, expression)
                else:
                    expr = expr.replace(index_id, '({0})'.format(expression))
    if re.findall('\$.*?\$', expr):
        return parse_express(expr)
    else:
        return expr


if __name__ == '__main__':
    # callcode("calc_class_ratio(exec_algor($1320$, ['福利还不错，年底双薪，有调休，能锻炼人',"
    #          "'太差劲', '福利很好，员工素质很好','福利很好，素质很好']), u'1')", {})

    print(callcode('$1389$', {
        'company': '康师傅'
    }))
