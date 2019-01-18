#!/usr/bin/env python
# coding: utf-8 

import json
import os
import logging

logger = logging.getLogger(__name__)


def parse_json(filename):
    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        logger.info(f"File {filename} does not exist.")


def chunk(list, chunksize):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(list), chunksize):
        yield list[i:i + chunksize]