#!/usr/bin/env python
# coding: utf-8 

import json
import os
import logging
from contextlib import ExitStack
import pathlib
import importlib_resources

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


def get_pkg_resource(
    file_manager: ExitStack, path: str, package: str = "impresso_commons"
) -> pathlib.PosixPath:
    """Return the resource at `path` in `package`, using a context manager.

    Note: 
        The context manager `file_manager` needs to be instantiated prior to 
        calling this function and should be closed once the package resource 
        is no longer of use.

    Args:
        file_manager (contextlib.ExitStack): Context manager.
        path (str): Path to the desired resource in given package.
        package (str, optional): Package name. Defaults to "impresso_commons".

    Returns:
        pathlib.PosixPath: Path to desired managed resource.
    """
    ref = importlib_resources.files(package)/path
    return file_manager.enter_context(importlib_resources.as_file(ref))