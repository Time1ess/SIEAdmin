import argparse
import logging
import os
import signal
import sys
import time

from collections import defaultdict

from core.system import system_status, get_uid
from core.daemon import Daemon
from utils import build_rescaler, round_by
from config import config


class PriorityScheduler(Daemon):
    """Update processes priority dynamically."""
    ss = system_status
    __exit_now = False
    __exited = False

    def __init__(self):
        """Initialization."""
        pidfile = config['cpu']['pidfile']
        super(PriorityScheduler, self).__init__(pidfile)
        self.cpu_intervene = config.getint('cpu', 'cpu_intervene')
        self.ram_intervene = config.getint('cpu', 'ram_intervene')
        self.interval = config.getint('cpu', 'interval')
        scheduler = config['cpu']['scheduler']
        self.scheduler = getattr(self, scheduler, None)
        if self.scheduler is None:
            logging.critical('No such scheduler')
            raise AttributeError('No such scheduler')
        signal.signal(signal.SIGTERM, self.__exit)

    def __exit(self, *args, **kwargs):
        """SIGTERM handler."""
        if self.__exit_now:
            return
        logging.info('>>> PriorityScheduler <<< deactivating')
        self.__exit_now = True

    def load_stats(self):
        """Load process status from SystemStatus."""
        logging.debug('Load stats')
        process_states = self.ss.process_states
        valid_users = os.listdir('/home')
        processes = defaultdict(list)
        user_processes_cnt = defaultdict(int)
        for ps in process_states:
            if ps.user in valid_users:
                processes[ps.user].append(ps)
                user_processes_cnt[ps.user] += 1
        return processes, user_processes_cnt

    def user_cpu_fair_scheduler(self, *stats):
        """Fair scheduler for users.

        Guarantee that each user will have fair CPU computing power.
        """
        logging.debug('User cpu fair scheduler called')
        processes, cnts, *_ = stats
        min_user_processes = min(cnts.values())
        priorities = {}
        for user, user_processes in cnts.items():
            user_ni = user_processes / min_user_processes
            total_process_weight = sum(p.cpu for p in processes[user])
            for p in processes[user]:
                process_ni = round_by(total_process_weight / p.cpu, 100) or 1
                priorities[p.pid] = process_ni * user_ni
        rescaler = build_rescaler(1, max(priorities.values()), 0, 19)
        priorities = {pid: round(rescaler(pri))
                      for pid, pri in priorities.items()}
        for pid, pri in priorities.items():
            self.__renice(pid=pid, pri=pri)

    def user_ram_penalty_scheduler(self, *stats):
        """Penalty scheduler for RAM.

        Punish users who use RAM more than <ram_intervene> %
        by renice process to 19(lowest priority).
        """
        logging.debug('User ram penalty scheduler called')
        processes, *_ = stats
        user_ram = defaultdict(float)
        for user, user_processes in processes.items():
            for p in user_processes:
                user_ram[user] += p.mem
        for user, ram in user_ram.items():
            if ram > self.ram_intervene:
                uid = get_uid(user)
                self.__renice(uid=uid, pri=19)

    def cpu_ram_hybrid_scheduler(self, *stats):
        """Append user_ram_penalty_scheduler after user_cpu_fair_scheduler."""
        logging.debug('CPU ram hybrid scheduler called')
        self.user_cpu_fair_scheduler(*stats)
        self.user_ram_penalty_scheduler(*stats)

    @staticmethod
    def __renice(pid=None, uid=None, pri=0):
        """Renice by pid or uid."""
        if not pid and not uid:
            raise ValueError('Must provide pid or uid')
        elif pid:
            cmd = 'renice -n %d -p %d' % (pri, pid)
        else:
            cmd = 'renice -n %d -u %d' % (pri, uid)
        logging.debug(cmd)
        os.popen(cmd)

    def none_scheduler(self, *stats):
        """Reset all niceness to 0."""
        logging.debug('None scheduler called')
        processes, *_ = stats
        for user, processes in processes.items():
            for process in processes:
                pid = process.pid
                self.__renice(pid=pid)

    def run(self):
        """Scheduler core."""
        scheduler_name = self.scheduler.__name__
        logging.info('>>> PriorityScheduler <<< activated'
                     '(load: %d, interval: %d, scheduler: %s)' % (
                         self.cpu_intervene, self.interval, scheduler_name))
        try:
            while True:
                load_1m, load_5m, load_15m = self.ss.system_load
                if load_1m < self.cpu_intervene:
                    msg = 'CPU idle(%f, %f, %f), go to sleep.' % (
                        load_1m, load_5m, load_15m)
                    logging.info(msg)
                else:
                    stats = self.load_stats()
                    self.scheduler(*stats)
                    msg = '<%s> finished, go to sleep.' % scheduler_name
                    logging.info(msg)
                for _ in range(int(self.interval)):
                    if self.__exit_now:
                        return
                    time.sleep(1)
        except Exception as e:
            logging.error(e)
        finally:
            logging.info('Restoring niceness.')
            stats = self.load_stats()
            self.none_scheduler(*stats)
            self.__exited = True
            logging.info('>>> PriorityScheduler <<< deactivated')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['start', 'stop', 'restart'])
    args = parser.parse_args()
    daemon = PriorityScheduler()
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
