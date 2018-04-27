import argparse
import os
import os.path as osp
import re
import signal
import sys
import time
import logging


import utils

from core.daemon import Daemon
from core.system import system_status


class DiskUsageMonitor(Daemon):
    """Monitor disk usage and respond."""
    ss = system_status
    __exit_now = False
    __exited = False

    def __init__(self, user_quota='20G', interval=60,
                 pidfile='/tmp/SIE_disk_usage_monitord.pid'):
        """Initialization.

        Parameters
        ----------
        user_quota: str
            Quota of home size per user. Default: '20G'.
        interval: int
            How often the monitor should run. Default: 60 (seconds).
        """
        super(DiskUsageMonitor, self).__init__(pidfile)
        num, unit = re.findall(r'(\d+)([kKmMgG]{1})', user_quota)[0]
        quota_bytes = int(num)
        if unit in 'mM':
            quota_bytes *= 1024
        elif unit in 'gG':
            quota_bytes *= 1024 ** 2
        self.quota_bytes = quota_bytes
        self._quota = user_quota
        self.interval = interval
        signal.signal(signal.SIGTERM, self.__exit)

    def __exit(self, *args, **kwargs):
        """SIGTERM handler."""
        if self.__exit_now:
            return
        logging.info('>>> DiskUsageMonitor <<< deactivating')
        self.__exit_now = True

    @staticmethod
    def is_critical_process(p):
        """Check whether the process is critical."""
        if p.command in ('systemd', '(sd-pam)', 'sshd', 'sh', 'zsh',
                         'bash', 'tmux', 'vim', 'nano', 'ssh-agent', 'ssh',
                         'rm', 'mv', 'ls', 'cd', 'autossh'):
            return True
        return False

    @staticmethod
    def __kill_process(pid):
        """Kill process."""
        cmd = 'kill -9 %d' % pid
        logging.debug(cmd)
        os.popen(cmd)

    def kill_quota_exceeded_processes(self, disk_usage):
        """Kill all processes belong to quota-exceeded users."""
        logging.debug('Kill processes')
        for user, usage in disk_usage.items():
            if usage <= self.quota_bytes:
                continue
            logging.info('Quota exceeded User: %s' % user)
            processes = (p for p in self.ss.process_states
                         if p.user == user and not self.is_critical_process(p))
            for p in processes:
                self.__kill_process(p.pid)

    def load_usage(self):
        """Load disk usage."""
        logging.debug('Load usage')
        base_dir = '/home'
        disk_usage = {}
        for user_name in os.listdir(base_dir):
            path = osp.join(base_dir, user_name)
            if user_name.startswith('.'):
                continue
            usage, _ = os.popen('du -s %s' % path).read().split()
            usage = int(usage)
            disk_usage[user_name] = usage
        return disk_usage

    def run(self):
        """Monitor core."""
        logging.info('>>> DiskUsageMonitor <<< activated'
                     '(quota: %s, interval: %d)' % (self._quota,
                                                    self.interval))
        try:
            while True:
                disk_usage = self.load_usage()
                self.kill_quota_exceeded_processes(disk_usage)
                msg = 'Disk check finished, go to sleep.'
                logging.info(msg)
                for _ in range(int(self.interval)):
                    if self.__exit_now:
                        return
                    time.sleep(1)
        except Exception as e:
            logging.error(e)
        finally:
            self.__exited = True
            logging.info('>>> DiskUsageMonitor <<< deactivated')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['start', 'stop', 'restart'])
    parser.add_argument('--user_quota', default='20G', type=str)
    parser.add_argument('--interval', default=60, type=int)
    args = parser.parse_args()
    daemon = DiskUsageMonitor(user_quota=args.user_quota,
                              interval=args.interval)
    if 'start' == args.command:
        daemon.start()
    elif 'stop' == args.command:
        daemon.stop()
    elif 'restart' == args.command:
        daemon.restart()
    sys.exit(0)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)
