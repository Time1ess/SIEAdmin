import argparse
import logging
import os
import signal
import sys
import time

from collections import defaultdict

from core.system import SystemStatus
from core.daemon import Daemon
from utils import build_rescaler, round_by


class PrioritiyScheduler(Daemon):
    """Update processes priority dynamically."""
    ss = SystemStatus()
    __exit_now = False
    __exited = False


    def __init__(self, intervene_load=20, interval=30,
                 pidfile='/tmp/SIE_priority_schedulerd.pid'):
        super(PrioritiyScheduler, self).__init__(pidfile)
        self.intervene_load = intervene_load
        self.interval = interval
        signal.signal(signal.SIGTERM, self.__exit)

    def __exit(self, *args, **kwargs):
        if self.__exit_now:
            return
        logging.info('>>> PrioritiyScheduler <<< deactivating')
        self.__exit_now = True

    def load_stats(self):
        process_states = self.ss.process_states
        valid_users = os.listdir('/home')
        processes = defaultdict(list)
        user_processes_cnt = defaultdict(int)
        for ps in process_states:
            if ps.user in valid_users:
                processes[ps.user].append(ps)
                user_processes_cnt[ps.user] += 1
        return processes, user_processes_cnt

    def user_fair_scheduler(self, *stats):
        """Faier scheduler for users.

        Guarantee that each user will have fair CPU computing power.
        """
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
            self.__renice(pid, pri)

    @staticmethod
    def __renice(pid, pri=0):
        cmd = 'renice -n %d -p %d' % (pri, pid)
        logging.debug(cmd)
        os.popen(cmd)

    def none_scheduler(self, *stats):
        """Reset all niceness to 0."""
        processes, *_ = stats
        for user, processes in processes.items():
            for process in processes:
                pid = process.pid
                self.__renice(pid)

    def run(self, scheduler='user_fair_scheduler'):
        logging.info('>>> PrioritiyScheduler <<< activated'
                     '(load: %d, interval: %d)' % (self.intervene_load,
                                                   self.interval))
        scheduler = getattr(self, scheduler, None)
        if scheduler is None:
            logging.critical('No such scheduler')
            raise AttributeError('No such scheduler')
        try:
            while True:
                load_1m, load_5m, load_15m = self.ss.system_load
                if load_1m < self.intervene_load:
                    msg = 'CPU idle(%f, %f, %f), go to sleep.' % (
                        load_1m, load_5m, load_15m)
                    logging.info(msg)
                else:
                    stats = self.load_stats()
                    scheduler(*stats)
                    msg = 'Renice finished, go to sleep.'
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
            logging.info('>>> PrioritiyScheduler <<< deactivated')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['start', 'stop', 'restart'])
    parser.add_argument('--intervene_load', default=20, type=float)
    parser.add_argument('--interval', default=30, type=int)
    args = parser.parse_args()
    daemon = PrioritiyScheduler(interval=args.interval,
                                intervene_load=args.intervene_load)
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
