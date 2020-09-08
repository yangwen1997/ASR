# coding=utf-8
# 获取并保存数据库中表的元数据
import contextlib
import mysql.connector
from mysql.connector import errorcode
from util.log import logger
from dmp.store.db import default_sql as dest_db_conn


def get_tree_list():
    sqlstr = """SELECT `id`, 1 `pid`, `host` name, 1 `level` FROM
	t_data_source where user_name is NULL
UNION ALL
SELECT t1.`id`, t2.`id`, t1.`database_name`, 2 FROM
	t_data_source t1 inner join (select `id`,	1 `pid`, `host` `name`, 1 `level`
FROM
	t_data_source where user_name is null) t2 on t1.`host` = t2.`name` where t1.user_name is not null
UNION ALL
	SELECT b.id, a.id, b.table_name, 3 FROM t_table b
	INNER JOIN t_data_source a ON b.data_source_id = a.id"""
    result = dest_db_conn.query(sqlstr, dict_flag=True)
    return {'result': True, 'data': result}


def save_datasource(source_db):
    sqlstr = 'select ifnull(sum(flag),0) flag from (select 1 flag from t_data_source where `host`=%(host)s and ' \
             '`port`=%(port)s and `database_name`=%(database_name)s union all select 2 from t_data_source where ' \
             '`host`=%(host)s and `port` is null and `database_name` is null) a'
    result = dest_db_conn.query(sqlstr, source_db)[0][0]
    if result == 3:
        return {'result': False, 'message': '记录重复'}
    else:
        if result == 0:
            sqlstr = 'insert into t_data_source(data_source_type, data_source_name, `host`, `port`, `user_name`, `password`, ' \
                     'comment, `database_name`) values(%(data_source_type)s, %(data_source_name)s, %(host)s, %(port)s, ' \
                     '%(user_name)s, %(password)s, %(comment)s, %(database_name)s), (null, null, %(host)s, null, null, null, null, null)'
        elif result == 1:
            sqlstr = 'insert into t_data_source(data_source_type, data_source_name, `host`, `port`, `user_name`, `password`, ' \
                     'comment, `database_name`) values(null, null, %(host)s, null, null, null, null, null)'
        elif result == 2:
            sqlstr = 'insert into t_data_source(data_source_type, data_source_name, `host`, `port`, `user_name`, `password`, ' \
                     'comment, `database_name`) values(%(data_source_type)s, %(data_source_name)s, %(host)s, %(port)s, ' \
                     '%(user_name)s, %(password)s, %(comment)s, %(database_name)s)'
        (result, rowcount, rowid) = dest_db_conn.execute(sqlstr, source_db, effect_row=True, return_last_rowid=True)
        if result:
            return {'result': True, 'rowcount': rowcount, 'message': '保存记录提条数:{0}'.format(rowcount), 'id': rowid}
        else:
            return {'result': False, 'message': '执行保存操作出错'}


def update_datasource(source_db):
    sqlstr = 'update t_data_source set data_source_type=%(data_source_type)s, data_source_name=%(data_source_name)s, ' \
             'host=%(host)s, port=%(port)s, user_name=%(user_name)s, password=%(password)s, comment=%(comment)s, ' \
             'database_name=%(database_name)s) where id=%(id)s'
    (result, rowcount) = dest_db_conn.execute(sqlstr, source_db, effect_row=True)
    if result:
        return {'result': True, 'rowcount': rowcount, 'message': '更新记录提条数:{0}'.format(rowcount)}
    else:
        return {'result': False, 'message': '执行更新操作出错'}


# 当传入的id有顶级id时，会查询顶级id所有子节点一起删除
def delete_datasource(source_db):
    message = []
    for id in source_db['id']:
        sqlstr = 'select id from t_data_source where `host` in (select distinct `host` from t_data_source ' \
                 'where id=%(id)s and user_name is null) union select id from t_data_source where id=%(id)s'
        result = dest_db_conn.query(sqlstr, {'id': id})
        result_id = [x[0] for x in result]
        id_list = ',' + ','.join(map(str, result_id)) + ','

        sqlstr = 'delete d, t, c from t_data_source d left join t_table t on d.id = t.data_source_id ' \
                 'left join t_table_column c on t.id = c.table_id where instr(%(id_list)s, concat(\',\', d.id, \',\'))>0'
        (result, rowcount) = dest_db_conn.execute(sqlstr, {'id': id, 'id_list': id_list}, effect_row=True)
        if result:
            message.append(
                {'datasource_id': id, 'result': True, 'rowcount': rowcount, 'message': '删除记录提条数:{0}'.format(rowcount)})
        else:
            message.append({'datasource_id': id, 'result': False, 'message': '执行删除操作出错'})
    return {'result': True, 'message': message}


def test_datasource(source_db):
    result = False
    try:
        source_db_conn = mysql.connector.connect(user=source_db['user_name'],
                                                 password=source_db['password'],
                                                 host=source_db['host'],
                                                 port=source_db['port'],
                                                 database=source_db['database_name'])
        result = True
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Database does not exist")
        else:
            logger.error(err)
    return {'result': result}


def get_datasource(source_db_id):
    sql = 'select `id`, `host`, `port`, `database_name`, `user_name`, `password` from t_data_source where `id`=%(id)s'
    result = dest_db_conn.query(sql, {'id': source_db_id}, dict_flag=True)
    if result:
        return result[0]
    else:
        return None


def query_datasource(source_db):
    id = source_db['id'][0]
    result = get_datasource(id)
    return {'result': True, 'data': result}


def sync_table_metadata(source_db):
    for id in source_db['id']:
        db_info = get_datasource(id)
        source_db_conn = mysql.connector.connect(user=db_info['user_name'],
                                                 password=db_info['password'],
                                                 host=db_info['host'],
                                                 port=db_info['port'],
                                                 database=db_info['database_name'])
        with contextlib.closing(source_db_conn.cursor(dictionary=True)) as cursor:
            sqlstr = 'select {id} id, table_schema, table_name, table_type, table_comment from information_schema.tables ' \
                     'where table_schema = %(database_name)s'.format(id=id)
            cursor.execute(sqlstr, db_info)
            result = cursor.fetchall()
            sqlstr = 'insert into t_table(`data_source_id`, `schema`, `table_name`, `table_type`, `comment`) ' \
                     'values(%(id)s, %(table_schema)s, %(table_name)s, %(table_type)s, %(table_comment)s)'
            result, rowcount = dest_db_conn.batch_insert(sqlstr, result, effect_row=True)
            sqlstr = 'select `id`, `schema`, `table_name` from t_table where data_source_id=%s'
            result = dest_db_conn.query(sqlstr, [db_info['id']], dict_flag=True)
            for x in result:
                sqlstr = 'select {id} id, column_name, column_type, column_comment from information_schema.columns ' \
                         'where table_name=%(table_name)s and table_schema=%(schema)s'.format(id=x['id'])
                cursor.execute(sqlstr, x)
                result = cursor.fetchall()
                sqlstr = 'insert into t_table_column(`table_id`, `column_name`, `column_type`, `column_comment`) ' \
                         'values(%(id)s, %(column_name)s, %(column_type)s, %(column_comment)s)'
                dest_db_conn.batch_insert(sqlstr, result)
    return {'result': True, 'rowcount': 'rowcount'}


def update_table_info(source_db):
    sqlstr = 'update t_table(comment, label) values %(comment, label)s where id=%(id)s'
    result = dest_db_conn.execute(sqlstr, source_db)
    return {'result': result}


def update_column_info(source_db):
    sqlstr = 'update t_table_column(column_comment) values %(column_comment)s where id=%(id)s'
    result = dest_db_conn.execute(sqlstr, source_db)
    return {'result': result}


def get_table_info(source_db):
    message = dict()
    id = {'id': source_db['id'][0]}
    sqlstr = 'select table_name, comment, label from t_table where id=%(id)s'
    result = dest_db_conn.query(sqlstr, id, dict_flag=True)
    if result:
        message['table'] = result[0]
    sqlstr = 'select column_name, column_type, column_comment from t_table_column where table_id=%(id)s'
    result = dest_db_conn.query(sqlstr, id, dict_flag=True)
    message['columns'] = result
    message['result'] = True
    return message
