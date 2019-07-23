import dask
import os
from impresso_commons.path.path_s3 import impresso_iter_bucket
from impresso_commons.text.rebuilder import rebuild_issues, compress
from impresso_commons.path.path_s3 import read_s3_issues
from dask.distributed import Client
import pkg_resources
import logging

logger = logging.getLogger(__name__)

DASK_WORKERS_NUMBER = 36
DASK_MEMORY_LIMIT = "1G"

# Use an env var to determine the type of dask scheduling to run:
# 1) synchronous; distributed external or distributed internal
try:
    DASK_SCHEDULER_STRATEGY = os.environ['PYTEST_DASK_SCHEDULER']
except KeyError:
    DASK_SCHEDULER_STRATEGY = 'internal'

if DASK_SCHEDULER_STRATEGY == 'internal':
    client = Client(
        processes=False,
        n_workers=DASK_WORKERS_NUMBER,
        threads_per_worker=1,
        memory_limit=DASK_MEMORY_LIMIT
    )
    print(f"Dask client {client}")
    print(f"Dask client {client.scheduler_info()['services']}")

elif DASK_SCHEDULER_STRATEGY == 'synchronous':
    # it does not work perfectly but almost
    dask.config.set(scheduler="synchronous")

elif DASK_SCHEDULER_STRATEGY == 'external':
    client = Client('localhost:8786')


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
    input_bucket_name = "s3://original-canonical-compressed"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = read_s3_issues("JDG", "1830", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

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

def test_rebuild_JDG2():
    input_bucket_name = "s3://original-canonical-fixed"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )
    
    input_issues = read_s3_issues("JDG", "1862", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

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
    input_bucket_name = "s3://original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = read_s3_issues("GDL", "1806", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

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


def test_rebuild_IMP():
    input_bucket_name = "s3://original-canonical-compressed"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = read_s3_issues("IMP", "1994", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:50],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr'
    )
    logger.info(json_files)
    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_luxzeit1858():
    input_bucket_name = "s3://original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = read_s3_issues("luxzeit1858", "1858", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:50],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_indeplux():
    input_bucket_name = "s3://TRANSFER"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = read_s3_issues("indeplux", "1905", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:50],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr',
        filter_language=['fr']
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_luxwort():
    input_bucket_name = "s3://original-canonical-data"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt'
    )

    input_issues = read_s3_issues("luxwort", "1852", input_bucket_name)
    print(f'{len(input_issues)} issues to rebuild')

    issue_key, json_files = rebuild_issues(
        issues=input_issues,
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=None,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None


def test_rebuild_for_passim():
    input_bucket_name = "s3://original-canonical-compressed"
    outp_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/rebuilt-passim'
    )

    input_issues = read_s3_issues("IMP", "1982", input_bucket_name)

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:50],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='passim'
    )
    logger.info(f'{issue_key}: {json_files}')
