# -*- coding: utf-8 -*-
import time
import subprocess

import arrow

from my_yaml import MyYAML
from my_logger import *


debug_logging('/var/logs/error.log')
logger = logging.getLogger('root')


class Dumper(object):
    def __init__(self):
        self.ini = MyYAML('my.yaml')
        self.my_ini = self.ini.get_ini()
        self.flag_ini = MyYAML('flag.yaml')
        self.last_time = arrow.get(self.flag_ini.get_ini()['last_time'])
        #self.last_time = arrow.now('PRC')

    def set_flag(self, time, msg=''):
        self.last_time = time
        self.flag_ini.set_ini({'last_time': time.format('YYYY-MM-DDTHH:mm:ssZZ')})
        logger.info('{0} {1}'.format(time.format('YYYY-MM-DDTHH:mm:ssZZ'), msg))

    def dump(self, ini, date_string):
        cmd = '/root/tidb-enterprise-tools-latest-linux-amd64/bin/mydumper -h {0} -P {1} -u {2} -p {3} -t 16 -F 64 -B {4} --skip-tz-utc -o /var/dump/{5}/{6}'.format(ini['host'], ini['port'], ini['user'], ini['pwd'], ini['db'], ini['db'], date_string)
        child = subprocess.Popen(cmd, shell=True)
        child.wait()
        print(cmd)
        logger.info(cmd)
        logger.info('{0} has been created'.format(date_string))
    
    def time_check(self):
        now = arrow.now('PRC')
        if(now.timestamp - self.last_time.timestamp) < float(60*60):
            return
        self.dump(dict(self.my_ini['mysql']), now.format('YYYY-MM-DD_HH:mm:ss'))
        self.set_flag(now)

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