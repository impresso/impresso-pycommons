#!/usr/bin/env python3
# coding: utf-8


from impresso_commons.path.path_s3 import (s3_select_contentitems,
                                           s3_select_issues,
                                           s3_detect_issues,
                                           s3_detect_contentitems,
                                           impresso_iter_bucket)
from impresso_commons.utils.s3 import get_bucket


def test_s3_select_contentitems():
    rebuilt_bucket = get_bucket('canonical-rebuilt-versioned', create=False)

    config = {
        "GDL": [1950, 1952],
        "BDC": []
    }

    keys = s3_select_contentitems(rebuilt_bucket, config)

    assert keys is not None
    assert len(keys) > 0


def test_s3_select_issues():
    canonical_bucket = get_bucket('canonical-json', create=False)

    config = {
        "GDL": [1910, 1912],
        "BDC": []
    }

    keys = s3_select_issues(canonical_bucket, config)

    assert keys is not None
    assert len(keys) > 0


def test_s3_detect_issues():
    canonical_bucket = get_bucket('canonical-json', create=False)
    keys = s3_detect_issues(canonical_bucket, "GDL/1822/01")
    assert keys is not None
    assert len(keys) > 0


def test_s3_detect_contentitems():
    canonical_bucket = get_bucket('canonical-rebuilt-versioned', create=False)
    keys = s3_detect_contentitems(canonical_bucket, "GDL/1950/01")
    assert keys is not None
    assert len(keys) > 0


def test_impresso_iter_bucket():
    np_config = {
        "GDL": [1950, 1951],
    }

    iter_items = impresso_iter_bucket(
        bucket_name="canonical-rebuilt-versioned",
        item_type="content_item",
        # prefix = 'GDL/1950'
        filter_config=np_config
    )
    assert iter_items is not None
    assert len(iter_items) > 0
