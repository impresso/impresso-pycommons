from impresso_commons.utils.s3 import get_bucket, get_s3_versions, read_jsonlines
import dask.bag as db
import json
import pytest


def test_get_s3_versions():
    bucket_name = "canonical-rebuilt"
    bucket = get_bucket(bucket_name)
    keys = bucket.get_all_keys()[:10]
    info = [
        get_s3_versions(bucket_name, key.name)
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



