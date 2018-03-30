#!/usr/bin/env python
# coding: utf-8 
# created on 2018.03.27 using PyCharm 
# project impresso-image-acquisition


import logging

import dask
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from dask.multiprocessing import get as mp_get

__author__ = "maudehrmann"


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

    if log_file is not None:
        handler = logging.FileHandler(filename=log_file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Logger successfully initialised")
