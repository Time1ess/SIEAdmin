import argparse
import logging
import os
import signal
import sys
import time

from collections import defaultdict

from core.system import SystemStatus, get_uid
from core.daemon import Daemon
from utils import build_rescaler, round_by


class PrioritiyScheduler(Daemon):
    """Update processes priority dynamically."""
    ss = SystemStatus()
    __exit_now = False
    __exited = False

    def __init__(self, cpu_intervene=20, ram_intervene=40, interval=30,
                 pidfile='/tmp/SIE_priority_schedulerd.pid',
                 scheduler='user_cpu_fair_scheduler'):
        """
        Parameters
        ----------
        cpu_intervene: float
            Scheduler will intervene when average CPU load in 1 minute reach
            this value. Default: 20.
        ram_intervene: float
            Scheduler will punish user when user RAM reatch this value, only
            for user_ram_penalty_scheduler and cpu_ram_hybrid_scheduler.
            Default: 40 (in percent).
        interval: int
            How often the scheduler should run. Default: 30 (seconds).
        pidfile: str
            Path to pidfile. Default: /tmp/SIE_priority_schedulerd.pid.
        scheduler: str
            Which scheduler algorithm should use.
            Default: user_cpu_fair_scheduler.
        """
        super(PrioritiyScheduler, self).__init__(pidfile)
        self.cpu_intervene = cpu_intervene
        self.ram_intervene = ram_intervene
        self.interval = interval
        self.scheduler = getattr(self, scheduler, None)
        if self.scheduler is None:
            logging.critical('No such scheduler')
            raise AttributeError('No such scheduler')
        signal.signal(signal.SIGTERM, self.__exit)

    def __exit(self, *args, **kwargs):
        """SIGTERM handler."""
        if self.__exit_now:
            return
        logging.info('>>> PrioritiyScheduler <<< deactivating')
        self.__exit_now = True

    def load_stats(self):
        """Load process status from SystemStatus."""
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
        processes, *_ = stats
        user_ram = defaultdict(float)
        for user, user_processes in processes.items():
            for p in user_processes:
                user_ram[user] += p.mem
        logging.info(user_ram)
        for user, ram in user_ram.items():
            if ram > self.ram_intervene:
                uid = get_uid(user)
                self.__renice(uid=uid, pri=19)

    def cpu_ram_hybrid_scheduler(self, *stats):
        """Append user_ram_penalty_scheduler after user_cpu_fair_scheduler."""
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
        processes, *_ = stats
        for user, processes in processes.items():
            for process in processes:
                pid = process.pid
                self.__renice(pid=pid)

    def run(self):
        """Scheduler core."""
        logging.info('>>> PrioritiyScheduler <<< activated'
                     '(load: %d, interval: %d)' % (self.cpu_intervene,
                                                   self.interval))
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
    parser.add_argument('--cpu_intervene', default=20, type=float)
    parser.add_argument('--ram_intervene', default=40, type=float)
    parser.add_argument('--interval', default=30, type=int)
    parser.add_argument('--scheduler', default='cpu_ram_hybrid_scheduler',
                        type=str)
    args = parser.parse_args()
    daemon = PrioritiyScheduler(cpu_intervene=args.cpu_intervene,
                                ram_intervene=args.ram_intervene,
                                interval=args.interval,
                                scheduler=args.scheduler)
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
