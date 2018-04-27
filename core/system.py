import os
import re


class ProcessState(object):
    def __init__(self, pid, user, pr, ni, virt, res, shr,
                 s, cpu, mem, time, command):
        self.pid = int(pid)
        self.user = user
        self.pr = pr
        self.ni = ni
        self.virt = virt
        self.res = res
        self.shr = shr
        self.s = s
        self.cpu = float(cpu)
        self.mem = float(mem)
        self.command = command

    def __repr__(self):
        fmt = '<ProcessState PID: %d USER: %s CPU: %.1f MEM: %.1f COMMAND: %s'
        return fmt % (self.pid, self.user, self.cpu, self.mem, self.command)


class SystemStatus(object):
    @property
    def system_load(self):
        data = os.popen('uptime').read()
        load_pat = re.compile(r'load average: (.*?), (.*?), (.*?)$')
        return [float(x) for x in load_pat.findall(data)[0]]

    @property
    def process_states(self):
        data = os.popen(
            'top -b -u \'!root\' -d .1 -n 10').read()
        pat = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]|\x1b\(B|\x1b>')
        data = pat.sub('', data)
        data = data[data.rfind('top - '):].split('\n')[7:-1]
        states = [ProcessState(*x.split(maxsplit=11)) for x in data]
        data = os.popen('ps axo pid,user:20').read().split('\n')[1:-1]
        pid_to_user = dict(x.strip().split(maxsplit=1) for x in data)

        def valid_process_state(state):
            if str(state.pid) not in pid_to_user:
                return False
            if state.cpu < 10:
                return False
            return True

        states = [state for state in states if valid_process_state(state)]
        for state in states:
            state.user = pid_to_user[str(state.pid)]
        return states

    @property
    def _process_states(self):
        data = os.popen(
            'ps axo pid,user:20,pri,ni,vsz,rss,pcpu,pmem,comm').read()
        data = data.split('\n')[1:-1]
        return [ProcessState(*x.split(maxsplit=8)) for x in data]


if __name__ == '__main__':
    ss = SystemStatus()
    print(ss.system_load)
    print(ss.process_states)
