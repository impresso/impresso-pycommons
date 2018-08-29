from impresso_commons.path.path_s3 import impresso_iter_bucket
from impresso_commons.utils.s3 import get_bucket
from impresso_commons.text.rebuilder import rebuild_issues
import pkg_resources


def test_rebuild_issues():
    input_bucket_name = "original-canonical-data"
    bucket = get_bucket(input_bucket_name)
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    issues = impresso_iter_bucket(
        input_bucket_name,
        prefix="GDL/1950/01",
        item_type="issue"
    )

    result = rebuild_issues(
        issues,
        bucket,
        outp_dir,
        None,  # do not upload to s3
        True
    )

    assert result is not None
