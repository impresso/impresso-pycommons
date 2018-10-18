"""

Usage:
    impresso-db/dask_preprocessing.py partition --config-file=<cf> [--output-dir=<i> --log-file=<f> --verbose]
"""

import logging
import docopt
import os
import sys
import json
import bz2

from impresso_commons.utils import user_confirmation
from impresso_commons.utils.s3 import get_bucket, create_even_partitions


__author__ = "maudehrmann"

logger = logging.getLogger(__name__)


def init_logger(logger, log_level, log_file):  # todo: integrate defiitively in utils.
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


def main(args):

    # get args
    output_dir = args["--output-dir"]
    log_file = args["--log-file"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO
    config_file = args["--config-file"]

    # init logger
    global logger
    init_logger(logger, logging.INFO, log_file=None)
    logger.info(f"CLI arguments received: {args}")

    # load params from config file
    with open(config_file, 'r') as f:
        config_dict = json.load(f)
    try:
        bucket_name = config_dict["s3_bucket"]
        config_newspapers = config_dict["newspapers"]
    except KeyError:
        logger.critical(f"One of the key [s3_bucket|newspapers] is missing in config file.")

    # get the s3 bucket
    if "rebuilt" not in bucket_name:
        answer = user_confirmation("Text import should be based on rebuilt text, but bucket does not correspond."
                                   "Do you want to continue?", None)
        if answer:
            bucket = get_bucket(bucket_name, create=False)
    else:
        bucket = get_bucket(bucket_name, create=False)

    logger.info(f"Retrieved bucket: {bucket.name}")

    # processes
    if args["partition"] is True:
        create_even_partitions(bucket,
                               config_newspapers,
                               '/scratch/impresso/GDL_partitions',
                               "rebuilt-dask-partitions",
                               "GDL",
                               nb_partition=100)


if __name__ == "__main__":
    main()