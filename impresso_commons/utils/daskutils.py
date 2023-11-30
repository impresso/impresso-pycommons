#!/usr/bin/env python3
# coding: utf-8

"""
Utility which help preparing data in view of parallel or distributed computing, in a dask-oriented view.

Usage:
    daskutils.py partition --config-file=<cf> --nb-partitions=<p> [--log-file=<f> --verbose]

Options:
    --config-file=<cf>  json configuration dict specifying various arguments
"""

import logging
import docopt
import os

from dask.diagnostics import ProgressBar
import dask.bag as db
import numpy as np

from impresso_commons.utils import init_logger
from impresso_commons.utils import Timer, user_confirmation
from impresso_commons.path.path_s3 import s3_filter_archives
from impresso_commons.utils.s3 import get_bucket, read_jsonlines, readtext_jsonlines
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT
from impresso_commons.utils.config_loader import PartitionerConfig

__author__ = "maudehrmann"

logger = logging.getLogger(__name__)


def partitioner(bag, path, nbpart):
    """Partition a bag into n partitions and write each partition in a file"""
    grouped_items = bag.groupby(lambda x: np.random.randint(500), npartitions=nbpart)
    items = grouped_items.map(lambda x: x[1]).flatten()
    path = os.path.join(path, "*.jsonl.bz2")
    with ProgressBar():
        items.to_textfiles(path)


def create_even_partitions(bucket,
                           config_newspapers,
                           output_dir,
                           local_fs=False,
                           keep_full=False,
                           nb_partition=500):
    """Convert yearly bz2 archives to even bz2 archives, i.e. partitions.

    Enables efficient (distributed) processing, bypassing the size discrepancies of newspaper archives.
    N.B.: in resulting partitions articles are all shuffled.
    Warning: consider well the config_newspapers as it decides what will be in the partitions and loaded in memory.

    :param bucket: name of the bucket where the files to partition are
    :param config_newspapers: json dict specifying the sources to consider (name(s) of newspaper(s) and year span(s))
    :param output_dir: classic FS repository where to write the produced partitions
    :param local_fs:
    :param keep_full: whether to filter out metadata or not (i.e. keeping only text and leaving out coordinates)
    :param nb_partition: number of partitions
    :return: None
    """

    t = Timer()

    # set the output
    if local_fs:
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, "*.jsonl.bz2")
    else:
        path = f'{output_dir}/*.jsonl.gz'
    logger.info(f"Will write partitions to {path}")

    # collect (yearly) keys & load in bag
    bz2_keys = s3_filter_archives(bucket.name, config=config_newspapers)
    bag_bz2_keys = db.from_sequence(bz2_keys)

    # read and filter lines (1 elem = list of lines, or articles, from a key)
    if keep_full is False:
        bag_items = bag_bz2_keys.map(readtext_jsonlines, bucket_name=bucket.name).flatten()
    else:
        bag_items = bag_bz2_keys.map(read_jsonlines, bucket_name=bucket.name).flatten()

    # repartition evenly
    grouped_items = bag_items.groupby(lambda x: np.random.randint(1000), npartitions=nb_partition)
    items = grouped_items.map(lambda x: x[1]).flatten()

    # write partitions
    with ProgressBar():
        # items.compute()
        # if local_fs:
        #     items.to_textfiles(path)
        # else:
        items.to_textfiles(path,
                           storage_options=IMPRESSO_STORAGEOPT,
                           compute=True)

    logger.info(f"Partitioning done in {t.stop()}.")


def main(args):
    # get args
    config_file = args["--config-file"]
    log_file = args["--log-file"]
    nb_partitions = args["--nb-partitions"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO

    # init logger
    global logger
    logger = init_logger(logging.getLogger(), log_level, log_file)
    logger.info(f"CLI arguments received: {args}")

    config = PartitionerConfig.from_json(config_file)

    bucket = get_bucket(config.bucket_name, create=False)
    logger.info(f"Retrieved bucket: {bucket.name}")

    if args["partition"] is True:
        create_even_partitions(bucket,
                               config.newspapers,
                               config.output_dir,
                               local_fs=config.local_fs,
                               keep_full=config.keep_full,
                               nb_partition=int(nb_partitions))


if __name__ == "__main__":
    arguments = docopt.docopt(__doc__)
    main(arguments)
