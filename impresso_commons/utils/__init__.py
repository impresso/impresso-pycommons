#!/usr/bin/env python
# coding: utf-8
# created on 2018.03.27 using PyCharm
# project impresso-image-acquisition

import logging
import sys
import time
import datetime
from datetime import timedelta

import dask
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get as mp_get
import multiprocessing

logger = logging.getLogger(__name__)


def executetask(tasks, parallel_execution):
    """ Effectively run execution of tasks

    :param tasks                : list of tasks to be executed
    :type tasks                 : xx
    :param parallel_execution   :
    :type parallel_execution    : boolean ?
    """
    with ProgressBar():
        if parallel_execution:
            result = compute(*tasks, get=mp_get)
        else:
            result = compute(*tasks, get=dask.get)
    return result


def init_logger(logger, log_level, log_file):
    """Initialise the logger."""
    logger.setLevel(log_level)

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )

    if log_file is not None:
        fh = logging.FileHandler(filename=log_file, mode='w')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.info("Logger successfully initialised")

    return logger


def user_confirmation(question, default=None):
    """
    Ask a yes/no question via raw_input() and return their answer.

    @param question: a string that is presented to the user.
    @param default: the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).
    @return: True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")



def user_question(variable_to_confirm):
    answer = user_confirmation(f"Is [{variable_to_confirm}] the correct one to work with?", None)
    if not answer:
        logger.info(f"Variable {variable_to_confirm} not confirmed, exiting.")
        sys.exit()
    else:
        logger.info(f"Variable {variable_to_confirm} confirmed.")


def timestamp():
    """Returns an iso-formatted timestamp.

    :return: a timestamp
    :rtype: str
    """
    utcnow = datetime.datetime.utcnow()
    ts = int(utcnow.timestamp())
    d = datetime.datetime.fromtimestamp(ts)
    return d.isoformat() + "Z"


class Timer:
    """ Basic timer"""
    def __init__(self):
        self.start = time.time()
        self.intermediate = time.time()

    def tick(self):
        elapsed_time = time.time() - self.intermediate
        self.intermediate = time.time()
        return str(timedelta(seconds=elapsed_time))

    def stop(self):
        elapsed_time = time.time() - self.start
        return str(timedelta(seconds=elapsed_time))


def _get_cores():
    nb = multiprocessing.cpu_count()
    if nb >= 48:
        return nb - 10
    else:
        return nb


def chunk(list, chunksize):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(list), chunksize):
        yield list[i:i + chunksize]
