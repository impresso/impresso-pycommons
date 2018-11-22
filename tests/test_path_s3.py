#!/usr/bin/env python3
# coding: utf-8

import pytest
from impresso_commons.path.path_s3 import impresso_iter_bucket, s3_filter_archives, s3_iter_bucket
from impresso_commons.utils.s3 import get_bucket


def test_impresso_iter_bucket():
    np_config = {
        "GDL": [1950, 1951],
    }

    iter_items = impresso_iter_bucket(
        bucket_name="original-canonical-data",
        item_type="content_item",
        # prefix = 'GDL/1950'
        filter_config=np_config
    )
    assert iter_items is not None
    assert len(iter_items) > 0


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
