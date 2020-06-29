import pathlib
import logging
import datetime
from logging.handlers import RotatingFileHandler
import numpy as np


FDIR = pathlib.Path(__file__).parent.resolve()
LOG_DIR = FDIR / 'logs'


def str2path(str_path):
    if str_path is None:
        return None
    else:
        return pathlib.Path(str_path)


def simple_logger(logger_name='root', log_file_name=None):
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    if log_file_name:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / log_file_name
        file_handler = RotatingFileHandler(log_file.as_posix(), maxBytes=1048576)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        log.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    log.addHandler(stream_handler)

    return log


def interpolate_timestamps(start_ts, end_ts, count):
    timestamps_s = np.linspace(start_ts.timestamp(), end_ts.timestamp(), count+2)
    timestamps = np.array([datetime.datetime.fromtimestamp(ts) for ts in timestamps_s])
    return timestamps[1:-1]
