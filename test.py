# -*- coding: utf-8 -*-
import time
import subprocess

import arrow
from tinydb import TinyDB, Query

from my_yaml import MyYAML
from my_logger import *


debug_logging('logs/error.log')
logger = logging.getLogger('root')


class Dumper(object):
    def __init__(self):
        self.ini = MyYAML('my.yaml')
        self.my_ini = self.ini.get_ini()
        self.flag_ini = MyYAML('flag.yaml')
        self.last_time = arrow.get(self.flag_ini.get_ini()['last_time'])
        self.backup_path = self.my_ini['backup_path']
        self.interval = self.my_ini['interval']
        self.gc = self.my_ini['gc']
        db = TinyDB('db.json')
        self.table = db.table('dump')

    def set_flag(self, date, msg=''):
        self.last_time = date
        self.flag_ini.set_ini({'last_time': date.format('YYYY-MM-DDTHH:mm:ssZZ')})
        logger.info('{0} {1}'.format(date.format('YYYY-MM-DDTHH:mm:ssZZ'), msg))

    def dump(self, ini, date):
        # ±£´æÎ»ÖÃ
        folder = '{5}/{6}/{7}'.format(self.backup_path, ini['db'], date.format('YYYYMMDDTHHmmss'))
        # shellÃüÁî
        cmd = '/root/tidb-enterprise-tools-latest-linux-amd64/bin/mydumper -h {0} -P {1} -u {2} -p {3} -t 16 -F 64 -B {4} --skip-tz-utc -o {5}'.format(ini['host'], ini['port'], ini['user'], ini['pwd'], ini['db'], folder)
        child = subprocess.Popen(cmd, shell=True)
        child.wait()
        self.table.insert({'cmd': cmd, 'folder': folder, 'created_date': date.format('YYYY-MM-DDTHH:mm:ssZZ')})
        print(cmd)
        logger.info(cmd)


    def clean(self, gc):
        dumper = Query()
        items = self.table.all()
        if items == []:
            return
        created_date = arrow.get(items[0]['created_date'])
        if(time.time() - created_date.timestamp) > gc*24*60*60:
            self.table.remove(doc_ids=[items[0].doc_id])
            logger.info('{0} has been remove from TinyDB'.format(items[0]))
            
    
    def time_check(self):
        now = arrow.now('PRC')
        if(now.timestamp - self.last_time.timestamp) < self.interval*60*60.0:
            return
        self.dump(dict(self.my_ini['mysql']), now)
        self.set_flag(now)
        self.clean(self.gc)

    def run(self):
        while 1:
            try:
                self.time_check()
                time.sleep(5)
            except Exception as e:
                logger.error(e)
                time.sleep(30)

if __name__ == "__main__":
    d = Dumper()
    d.run()