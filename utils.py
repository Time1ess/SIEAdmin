"""Userful tools for SIE Server Admin."""
import logging

from config import config


logging.basicConfig(
    filename=config['logging']['logfile'],
    format=config['logging']['logfmt'],
    datefmt=config['logging']['datefmt'],
    level=logging.INFO)


def build_rescaler(src_min, src_max, tar_min, tar_max):
    """Build a rescaler to rescale [src_min, src_max] -> [tar_min, tar_max]."""
    def _wrapped(x):
        if src_min == src_max:
            return 0
        tar_ratio = (tar_max - tar_min) / (src_max - src_min)
        return tar_ratio * (x - src_min) + tar_min
    return _wrapped


def round_by(x, round_step):
    """Round a value to nearest k * round_step.

    Examples:
    ---------
    >>> round_by(165, 50)
    150
    >>> round_by(175, 50)
    200
    """
    return round(x / round_step) * round_step
