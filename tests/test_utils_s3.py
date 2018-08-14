from impresso_commons.utils.s3 import get_bucket, get_s3_versions


def test_get_s3_versions():
    bucket_name = "canonical-rebuilt-versioned"
    bucket = get_bucket(bucket_name)
    keys = bucket.get_all_keys()[:10]
    info = [
        get_s3_versions(bucket_name, key.name)
        for key in keys
    ]
    assert info is not None
    assert len(info) == len(keys)
