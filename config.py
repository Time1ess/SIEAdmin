import os.path as osp
import configparser


config = configparser.ConfigParser(interpolation=None)
config.read(osp.join(osp.abspath(osp.dirname(__file__)), 'SIEAdmin.ini'))
