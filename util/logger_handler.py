# coding= utf-8
import logging
import logging.config
from dmp.store.db import default_sql as db

class DBHandler(logging.Handler):
    def __init__(self, tablename):
        logging.Handler.__init__(self)
        self.SQL = """INSERT INTO {table} (
                        create_date,
                        log_level,
                        child_sys_id,
                        host_no,
                        monitor_name,
                        business_no,
                        business_name,
                        business_subject,
                        log_position,
                        log_content
                   )
                   VALUES (
                        %(create_date)s,
                        %(levelname)s,
                        %(child_sys_id)s,
                        %(host_no)s,
                        %(monitor_name)s,
                        %(business_no)s,
                        %(business_name)s,
                        %(business_subject)s,
                        %(log_position)s,
                        %(log_content)s
                   )
                   """.format(table=tablename)


    # TODO:
    def emit(self, record):
        self.format(record)
        record_dict = record.__dict__
        args = record_dict['args']
        log_dict = {
            'create_date': record_dict['asctime'],
            'levelname': record_dict['levelname'],
            'child_sys_id': args['child_sys_id'] if 'child_sys_id' in args else None,
            'host_no': args['host_no'] if 'host_no' in args else None,
            'monitor_name': args['monitor_name'] if 'monitor_name' in args else None,
            'business_no': args['business_no'] if 'business_no' in args else None,
            'business_name': args['business_name'] if 'business_name' in args else None,
            'business_subject': args['business_subject'] if 'business_subject' in args else None,
            'log_position': '{0}:{1}'.format(record_dict['filename'], record_dict['lineno']),
            'log_content': record_dict['message']
        }
        db.execute(self.SQL, log_dict)
