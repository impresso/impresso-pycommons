import dask
import os
import pytest
# global variables are imported from conftest.py
from conftest import S3_CANONICAL_BUCKET
from impresso_commons.text.rebuilder import rebuild_issues, compress
from impresso_commons.path.path_s3 import read_s3_issues
from impresso_commons.utils.utils import get_pkg_resource
from dask.distributed import Client
from contextlib import ExitStack
import logging

logger = logging.getLogger(__name__)

DASK_WORKERS_NUMBER = 8
DASK_MEMORY_LIMIT = "2G"

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
    client = None

elif DASK_SCHEDULER_STRATEGY == 'external':
    client = Client('localhost:8686')
    print(f"Dask client {client}")

limit_issues = 10
test_data = [
    ("NZZ", 1897, limit_issues),
    ("JDG", 1830, limit_issues),
    ("JDG", 1862, limit_issues),
    ("GDL", 1806, limit_issues),
    ("IMP", 1994, limit_issues),
    ("luxzeit1858", 1858, limit_issues),
    ("indeplux", 1905, limit_issues),
    ("luxwort", 1860, limit_issues),
    ("buergerbeamten", 1909, limit_issues),
    ("FedGazDe", 1849, limit_issues),
    ("excelsior", 1911, limit_issues),
    ("oecaen", 1914, limit_issues)

]

@pytest.mark.parametrize("newspaper_id, year, limit", test_data)
def test_rebuild_solr(newspaper_id : str, year : int, limit : int):
    file_mng = ExitStack()
    input_bucket_name = S3_CANONICAL_BUCKET
    outp_dir = get_pkg_resource(file_mng, 'data/rebuilt')

    input_issues = read_s3_issues(newspaper_id, year, input_bucket_name)
    print(f'{newspaper_id}/{year}: {len(input_issues)} issues to rebuild')
    print(f'limiting test rebuild to first {limit} issues.')

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:limit],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='solr'
    )

    result = compress(issue_key, json_files, outp_dir)
    logger.info(result)
    assert result is not None
    file_mng.close()

def test_rebuild_for_passim():
    input_bucket_name = S3_CANONICAL_BUCKET
    file_mng = ExitStack()
    outp_dir = get_pkg_resource(file_mng, 'data/rebuilt-passim')

    input_issues = read_s3_issues("luxwort", "1848", input_bucket_name)

    issue_key, json_files = rebuild_issues(
        issues=input_issues[:50],
        input_bucket=input_bucket_name,
        output_dir=outp_dir,
        dask_client=client,
        format='passim',
        filter_language=['fr']
    )
    logger.info(f'{issue_key}: {json_files}')
    file_mng.close()