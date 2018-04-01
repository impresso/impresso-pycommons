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
