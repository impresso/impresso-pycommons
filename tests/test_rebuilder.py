from impresso_commons.path.path_s3 import impresso_iter_bucket
from impresso_commons.text.rebuilder import rebuild_issues, compress
from dask.distributed import Client
import pkg_resources
import logging

logger = logging.getLogger(__name__)

client = Client(processes=False, n_workers=2, threads_per_worker=1)


def test_rebuild_NZZ():
    input_bucket_name = "original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = impresso_iter_bucket(
        input_bucket_name,
        prefix="NZZ/1784/12/",
        item_type="issue"
    )

    issue_key, json_files = rebuild_issues(
        issues=input_issues,
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_JDG():
    input_bucket_name = "original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = impresso_iter_bucket(
        input_bucket_name,
        prefix="JDG/1830/01/",
        item_type="issue"
    )

    issue_key, json_files = rebuild_issues(
        issues=input_issues,
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_GDL():
    input_bucket_name = "original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = impresso_iter_bucket(
        input_bucket_name,
        prefix="GDL/1799/01/0",
        item_type="issue"
    )

    issue_key, json_files = rebuild_issues(
        issues=input_issues,
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_for_passim():
    pass
