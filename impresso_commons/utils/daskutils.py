#!/usr/bin/env python3
# coding: utf-8

"""
Utility which help preparing data in view of parallel or distributed computing, in a dask-oriented view.

Usage:
    daskutils.py partition --config-file=<cf> --output-dir=<i> [--log-file=<f> --verbose]

Options:
    --config-file=<cf>  json configuration dict specifying various arguments
"""


import logging
import docopt
import os
import sys
import json
import bz2


from dask.diagnostics import ProgressBar
import dask.bag as db
import numpy as np

from impresso_commons.utils import Timer, user_confirmation
from impresso_commons.path.path_s3 import s3_filter_archives, get_s3_resource
from impresso_commons.utils.s3 import get_bucket, read_jsonlines, readtext_jsonlines, upload

__author__ = "maudehrmann"

logger = logging.getLogger(__name__)


def create_even_partitions(bucket,
                           config_newspapers,
                           output_dir,
                           partitioned_bucket_name,
                           partitioned_bucket_prefix,
                           keep_full=True,
                           nb_partition=500):
    """Convert yearly bz2 archives to even bz2 archives, i.e. partitions.

    Enables efficient (distributed) processing with dask, bypassing the size discrepancies of newspaper archives.
    N.B.: in resulting partitions articles are all shuffled.
    Warning: consider well the config_newspapers as it decides what will be in the partitions and loaded in memory.

    For now there is an intermediary step of writing to file system and before uploading on S3, will disappear when
    we could write directly on S3 via dask.

    @param bucket: name of the bucket where the files to partition are
    @param config_newspapers: json dict specifying the sources to consider (name(s) of newspaper(s) and year span(s))
    @param output_dir: classic FS repository where to write the produced partitions
    @param partitioned_bucket_name: the s3 bucket where to upload the partitions
    @param partitioned_bucket_prefix: the prefix of the key when uploading to S3 partitioned bucket
    @param keep_full: whether to filter out metadata or not (i.e. keeping only text and leaving out coordinates)
    @param nb_partition: number of partitions
    @return: None
    """

    t = Timer()

    # set the output (classic filesystem)
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "*.jsonl.bz2")
    logger.info(f"Will write partitions to {path}")

    # collect (yearly) keys
    bz2_keys = s3_filter_archives(bucket.name, config=config_newspapers)

    # load all bz2 archives
    bag_bz2_keys = db.from_sequence(bz2_keys)

    # read and filter lines (1 elem = list of lines, or articles, from a key)
    if keep_full is False:
        bag_articles = bag_bz2_keys.map(readtext_jsonlines, bucket_name=bucket.name).flatten()
    else:
        bag_articles = bag_bz2_keys.map(read_jsonlines, bucket_name=bucket.name).flatten()

    # classical repartition cmd does not produce even partitions => using a group by with a random variable
    grouped_articles = bag_articles.groupby(lambda x: np.random.randint(1000), npartitions=nb_partition)
    articles = grouped_articles.map(lambda x: x[1]).flatten()

    # write partitions on disk
    with ProgressBar():
        articles.to_textfiles(path)

    # todo: write partitions directly to S3
    logger.info(f"Partitioning done in {t.stop()}. Starting S3 upload")

    # upload to S3
    partitions = db.from_sequence([os.path.join(output_dir, file) for file in os.listdir(output_dir)])
    uploaded_partitions = partitions.map(upload,
                                         newspaper_prefix=partitioned_bucket_prefix,
                                         bucket_name=partitioned_bucket_name)
    with ProgressBar():
        uploaded_partitions.compute()

    logger.info(f"Total elapsed time: {t.stop()}")


def main(args):

    # get args
    log_file = args["--log-file"]
    log_level = logging.DEBUG if args["--verbose"] else logging.INFO
    config_file = args["--config-file"]

    # init logger
    global logger
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
    logger.info(f"CLI arguments received: {args}")

    # load params from config file
    with open(config_file, 'r') as f:
        config_dict = json.load(f)
    try:
        bucket_rebuilt = config_dict["s3_bucket_rebuilt"]
        bucket_partition = config_dict["s3_bucket_partitions"]
        dir_partition = config_dict["fs_partitions"]
        number_partitions = config_dict["number_partitions"]
        config_newspapers = config_dict["newspapers"]
    except KeyError:
        logger.critical(f"Not possible to parse config file.")

    # get the s3 bucket
    if "rebuilt" not in bucket_rebuilt:
        answer = user_confirmation("Partitioning should be based on rebuilt text, but bucket does not correspond."
                                   "Do you want to continue?", None)
        if answer:
            bucket = get_bucket(bucket_rebuilt, create=False)
    else:
        bucket = get_bucket(bucket_rebuilt, create=False)

    logger.info(f"Retrieved bucket: {bucket.name}")

    # processes
    if args["partition"] is True:
        create_even_partitions(bucket,
                               config_newspapers,
                               dir_partition,
                               bucket_partition,
                               "GDL",
                               nb_partition=number_partitions)


if __name__ == "__main__":
    main()