#!/usr/bin/env python3
# coding: utf-8

import pytest
from impresso_commons.path.path_s3 import impresso_iter_bucket


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
