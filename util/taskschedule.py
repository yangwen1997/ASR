# -*- coding: utf-8 -*-
'''
Created on 2016年4月25日

@author: 55Haitao
'''

import time
import mysql.connector,apscheduler
from util.utility import get_config
import logging,threadpool
from pytz import utc,country_timezones
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import random
from  dmp.preYSprice.json_parse_format4  import *

class taskschedule():
    def __init__(self):
        # _MYSQL_URL = get_config().get("apscheduler","URL")
        self.sched = BlockingScheduler({
            # 'apscheduler.jobstores.default': {
            # 'type': 'sqlalchemy',
            # 'url': _MYSQL_URL
            # },
            'apscheduler.executors.default': {
                'class': 'apscheduler.executors.pool:ThreadPoolExecutor',
                'max_workers': '20'
            },
            'apscheduler.executors.processpool': {
                'type': 'processpool',
                'max_workers': '5'
            },
            'apscheduler.job_defaults.coalesce': 'false',
            'apscheduler.job_defaults.max_instances': '3',
            # 'apscheduler.timezone': "UTC",   #标准国际时区
        })
        self.sched._logger = self.logging()
        self.sched.add_listener(self.err_listener, apscheduler.events.EVENT_JOB_ERROR | apscheduler.events.EVENT_JOB_MISSED)
    def logging(self):
        log = logging.getLogger('apscheduler.executors.default')
        log.setLevel(logging.INFO)  # DEBUG

        fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
        h = logging.StreamHandler()
        h.setFormatter(fmt)
        log.addHandler(h)
        return log

    def add_task(self,taskinfo,module_name,trigger="cron",misfire_grace_time=60,coalesce=False,max_instances=1,**trigger_model):
        exec ('from {0} import *'.format(module_name))
        # model_mata = __import__(module_name)
        # class_meta = getattr(model_mata,classname)
        # o = class_meta()
        fun_name = taskinfo['fun_name']
        trigger = taskinfo['trigger']   #触发方式，包括"cron,date,interval"
        # trigger_model = taskinfo["trigger_msodel"].split(" ")  #触发时间或者触发周期的设置
        params = taskinfo["param"]
        job_name = fun_name
        job_id = fun_name + str(time.time())[-6:]+ str(random.uniform(10, 20))
        if trigger == "cron":
            self.sched.add_job(eval(fun_name),trigger=trigger,args=params, name=job_name, id=job_id, max_instances=1, \
                            misfire_grace_time=misfire_grace_time, coalesce=True, replace_existing=False,**trigger_model)

        elif trigger == "interval":
            pass
        else:
            pass

    def err_listener(self,ev):
        err_logger = logging.getLogger('schedErrJob')
        if ev.exception:
            err_logger.exception('%s error.', str(ev.job))
        else:
            err_logger.info('%s miss', str(ev.job))


    def remove_task(self):
        self.sched.remove_job()

    def remove_tasks(self,job_id):
        self.sched.remove_all_jobs()

    def stop_tasks(self):
        self.sched.shutdown()

    def start_tasks(self):
        self.sched.start()

    def save_tasks(self):
        pass


if __name__ == '__main__':
    #示例说明
    ts = taskschedule()
    sched = ts.sched
    task = {}
    task['fun_name'] = "my_job_0"
    task["trigger"] = "cron"
    task["param"] = [{"a":1}]
    trigger_model_1 = {"minute":"*/1"}
    ts.add_task(task,"test.task1",**trigger_model_1)

    task['fun_name'] = "my_job_1"
    task["param"] = [{"b": 2}]
    trigger_model_2 = {"second": "*/5"}
    ts.add_task(task, "test.task1", **trigger_model_2)

    task['fun_name'] = "my_job_2"
    task["param"] = [{"c": 3}]
    trigger_model_3 = {"second": "*/10"}
    ts.add_task(task, "test.task1", **trigger_model_3)

    task['fun_name'] = "my_job_3"
    task["param"] = [{"d": 4}]
    trigger_model_4 = {"second": "*/20"}
    ts.add_task(task, "test.task1", **trigger_model_4)

    ts.start_tasks()
    # ts.stop_tasks()

    for item in ts.sched.get_jobs():
        print(item.id,item.next_run_time)
    #
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()
