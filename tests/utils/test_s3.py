import dask.bag as db
import json
import glob
import os
from contextlib import ExitStack

from impresso_commons.utils.utils import get_pkg_resource
from impresso_commons.utils.s3 import (get_bucket, get_s3_versions, 
                                       get_s3_client, read_jsonlines, 
                                       get_s3_connection, get_bucket_boto3, 
                                       get_or_create_bucket)
from impresso_commons.utils.daskutils import create_even_partitions
from impresso_commons.utils.config_loader import PartitionerConfig


def test_get_s3_versions():
    bucket_name = "canonical-rebuilt"
    s3c = get_s3_client()
    keys = s3c.list_objects(Bucket=bucket_name)['Contents'][:10]
    info = [
        get_s3_versions(bucket_name, key['Key'])
        for key in keys
    ]
    assert info is not None
    assert len(info) == len(keys)


def test_read_jsonlines():
    b = get_bucket("canonical-rebuilt", create=False)
    key = "GDL/GDL-1950.jsonl.bz2"
    lines = db.from_sequence(read_jsonlines(key, b.name))
    count_lines = lines.count().compute()
    some_lines = lines.map(json.loads).pluck('ft').take(10)

    assert count_lines is not None
    assert count_lines > 0
    assert some_lines is not None
    assert len(some_lines) > 0


def test_create_even_partitions():
    file_mng = ExitStack()
    dir_partition = get_pkg_resource(
        file_mng,
        'data/partitions/',
        package='impresso_commons'
    )

    config_newspapers = {
        "GDL": [1804, 1805]
    }
    keep_full = True,
    nb_partition = 100  # 500 on all data

    # get the s3 bucket
    bucket = get_bucket("canonical-rebuilt", create=False)
    create_even_partitions(bucket,
                           config_newspapers,
                           dir_partition,
                           local_fs=True,
                           keep_full=keep_full,
                           nb_partition=nb_partition)

    partitions = glob.glob(os.path.join(dir_partition, "*.bz2"))
    assert len(partitions) == 100
    file_mng.close()


def test_load_config():
    file_mng = ExitStack()
    file = get_pkg_resource(
        file_mng,
        'config/solr_ci_builder_config.example.json'
    )
    np = {'GDL': [1940, 1941]}
    assert file is not None
    if os.path.exists(file):
        config = PartitionerConfig.from_json(file)
        assert config is not None
        assert config.bucket_rebuilt == "canonical-rebuilt"
        assert config.newspapers == np
        assert config.local_fs == True
        assert config.keep_full == True
    file_mng.close()

def test_depreciation_warnings():
    # depreciation warning should show only once.
    s3c = get_s3_connection()
    bucket1 = get_bucket("canonical-data", create=False)
    bucket2 = get_bucket_boto3("canonical-data", create=False)
    none_bucket = get_or_create_bucket("canonical", versioning=False)

    assert s3c is not None
    assert bucket1 is not None
    assert bucket2 is not None
    assert none_bucket is None