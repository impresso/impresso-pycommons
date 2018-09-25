from impresso_commons.path.path_s3 import impresso_iter_bucket
from impresso_commons.text.rebuilder import rebuild_issues, compress
import pkg_resources
import logging

logger = logging.getLogger(__name__)


def test_rebuild_issues():
    input_bucket_name = "original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = impresso_iter_bucket(
        input_bucket_name,
        prefix="GDL/1950/01",
        item_type="issue"
    )

    issue_key, json_files = rebuild_issues(
        issues=input_issues,
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_scheduler=None,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None
