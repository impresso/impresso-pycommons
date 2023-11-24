#!/usr/bin/env python3
# coding: utf-8
import pytest
from impresso_commons.path.path_s3 import (impresso_iter_bucket,
                                           s3_filter_archives, s3_iter_bucket)
from impresso_commons.utils.s3 import get_bucket

impresso_iter_bucket_testdata = [
    #("original-canonical-data", "issue", None, 'GDL/issues/GDL-1950', 'not None'), 
    ("rebuilt-data", "item", {"GDL": [1950, 1951]}, None, 'Empty'),
    ("original-canonical-data", "item", {"GDL": [1950, 1951]}, 'GDL/pages/GDL-1950', 'None'),
]

@pytest.mark.parametrize(
    "bucket,type,np_config,prefix,expected", 
    impresso_iter_bucket_testdata
)
def test_impresso_iter_bucket(bucket, type, np_config, prefix, expected):

    iter_items = impresso_iter_bucket(
        bucket_name=bucket,
        item_type=type,
        prefix = prefix,
        filter_config=np_config
    )

    if expected == 'not None':
        assert iter_items is not None
        assert len(iter_items) > 0
    elif expected == 'Empty':
        assert len(iter_items) == 0
    else:
        assert iter_items is None


def test_s3_iter_bucket():
    b = get_bucket("canonical-rebuilt", create=False)
    keys = s3_iter_bucket(b.name, prefix='GDL', suffix=".bz2")
    assert keys is not None
    assert len(keys) > 0


def test_s3_filter_archives():
    config = {"GDL": [1950, 1959]}
    b = get_bucket("canonical-rebuilt", create=False)
    keys = s3_filter_archives(b.name, config=config)
    assert keys is not None
    assert len(keys) == 9


def test_s3_filter_archives_timebucket():
    config = {"GDL": [1799, 1999, 10]}
    b = get_bucket("canonical-rebuilt", create=False)
    keys = s3_filter_archives(b.name, config=config)
    assert keys is not None
    assert len(keys) == 20
