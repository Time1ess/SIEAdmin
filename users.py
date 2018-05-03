import argparse
import logging
import os
import os.path as osp
import sys
import time
import re

from flask import Flask, request

import utils

from core.daemon import Daemon
from config import config


app = Flask(__name__)


def read_processed_users():
    processed_users_file = config['users']['processed_users_file']
    if not osp.exists(processed_users_file):
        return {}
    with open(processed_users_file) as f:
        processed = dict([l.strip().split() for l in f])
    return processed


def write_processed(student_id, username):
    processed_users_file = config['users']['processed_users_file']
    with open(processed_users_file, 'a') as f:
        msg = '%s %s' % (student_id, username)
        f.write(msg + '\n')
    logging.info('Registration succeed: ' + msg)


def read_users():
    users_file = config['users']['users_file']
    if not osp.exists(users_file):
        return {}
    with open(users_file) as f:
        users = dict([l.strip().split() for l in f])
    return users


@app.route('/register', methods=['POST'])
def register():
    processed_users = read_processed_users()
    users = read_users()
    username = request.form['username'] or request.form['student_id']
    password = request.form['password']
    confirm_password = request.form['confirm_password']
    student_id = request.form['student_id']
    student_name = request.form['student_name']
    msg = ''
    if not username or re.sub(r'^[a-zA-Z0-9]+$', '', username) != '':
        msg = '用户名包含非法字符!'
    elif password != confirm_password:
        msg = '密码前后不一致!'
    elif student_id not in users:
        msg = '学号不在受邀注册范围内!'
    elif student_id in processed_users:
        msg = '学号已被注册!'
    elif student_name != users[student_id]:
        msg = '学号姓名不匹配!'
    if msg:
        logging.info(
            'Registration failed: ' + msg + '(%s)' % str(request.form))
        return msg
    try:
        p = os.popen('useradd -m %s -s /bin/bash' % username)
        if p.close() is not None:
            raise Exception()
        p = os.popen('echo "%s:%s" | chpasswd' % (username, password))
        if p.close() is not None:
            raise Exception()
        write_processed(student_id, username)
    except Exception as e:
        return '注册失败,请重试!(若重复出现，请联系系统管理员)'
    else:
        return '注册成功!'


@app.route('/')
def register_form():
    return '''
<script>
function check_input()
{
    var inputs = document.getElementsByTagName('input');
    for(var i = 1; i < inputs.length - 1; i++)
        if(inputs[i].value == '')
        {
            alert("请完成表单填写!");
            return false;
        }
    if(inputs[1].value != inputs[2].value)
    {
        alert("请确认两次密码一致!");
        return false;
    }
    return true;
}
</script>
<div style="text-align:center">
    <p1>自助注册</p1>
    <form method="POST" action="/register"  onSubmit="return check_input();">
    <p>
        <span>用户名:</span><input type="text" name="username" placeholder="不输入则默认为学号"/>
    </p>
    <p>
        <span>密码:</span><input type="password" name="password" />
    </p>
    <p>
        <span>确认密码:</span><input type="password" name="confirm_password" />
    </p>
    <p>
        <span>学号:</span><input type="text" name="student_id" />
    </p>
    <p>
        <span>姓名:</span><input type="text" name="student_name" />
    </p>
    <p>
        <input type="submit" />
    </p>
    </form>
</div>
'''


class UserRegistration(Daemon):
    def __init__(self):
        pidfile = config['users']['pidfile']
        super(UserRegistration, self).__init__(pidfile)

    def run(self):
        app.run('0.0.0.0')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['start', 'stop', 'restart'])
    args = parser.parse_args()
    daemon = UserRegistration()
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
