"""Code for parsing impresso's S3 directory structures."""

import os
import json
import logging
import warnings
from datetime import date
from collections import namedtuple

from dask.diagnostics import ProgressBar
import dask.bag as db

from impresso_commons.path import id2IssueDir
from impresso_commons.utils.s3 import get_s3_client, get_s3_versions, fixed_s3fs_glob
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT

logger = logging.getLogger(__name__)
_WARNED = False
# a simple data structure to represent input directories
IssueDir = namedtuple("IssueDirectory", ["journal", "date", "edition", "path"])


# a data structure to represent content items (articles or pages)
class s3ContentItem:
    def __init__(
        self,
        journal,
        date,
        edition,
        number,
        key_name,
        doc_type=None,
        rebuilt_version=None,
        canonical_version=None,
    ):
        self.journal = journal
        self.date = date
        self.edition = edition
        self.number = number
        self.doc_type = doc_type
        self.key_name = key_name
        self.rebuilt_version = rebuilt_version
        self.canonical_version = canonical_version


def _list_bucket_paginator(bucket_name, prefix="", accept_key=lambda k: True):
    """
    List the content of a bucket using pagination.
    No filtering besides indicated prefix and accept_key lambda.

    :param bucket_name: string, e.g. 'original-canonical-data'
    :param prefix: string, e.g. 'GDL/1950' - refers to the pseudo hierarchical
        structure within the bucket
    :param accept_key: lambda function, to accept or reject a specific key
    @return: arrays of keys
    """
    client = get_s3_client()
    paginator = client.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    keys = []
    for page in page_iterator:
        if "Contents" in page:
            for key in page["Contents"]:
                keyString = key["Key"]
                if accept_key(keyString):
                    keys.append(keyString)
    return keys if keys else []


def _list_bucket_paginator_filter(
    bucket_name, prefix="", accept_key=lambda k: True, config=None
):
    """
    List the content of a bucket using pagination, with a filter.
    :param bucket_name: string, e.g. 'original-canonical-data'
    :param prefix: string, e.g. 'GDL/1950' - refers to the pseudo hierarchical structure within the bucket
    :param accept_key: lambda function, to accept or reject a specific key
    :param config: a dict with newspaper acronyms as keys and array of year interval as values:
    e.g. { "GDL": [1950, 1960], "JDG": [1890, 1900] }. Last year is excluded.
    @return: arrays of keys
    """
    filtered_keys = []

    # building a list of prefixes from the config information
    for np in config:
        # if years are specified, take the range
        if config[np]:
            prefixes = [
                np + "/" + str(item) for item in range(config[np][0], config[np][1])
            ]
        # otherwise prefix is just the newspaper
        else:
            prefixes = [np]

        # retrieving keys using the prefixes
        print(f"Detecting items for {np} for years {prefixes}")
        for prefix in prefixes:
            client = get_s3_client()
            paginator = client.get_paginator("list_objects")
            page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            for page in page_iterator:
                if "Contents" in page:
                    for key in page["Contents"]:
                        keyString = key["Key"]
                        if accept_key(keyString):
                            filtered_keys.append(keyString)
    return filtered_keys if filtered_keys else []


def _key_to_issue(key_info):
    """Instantiate an IssueDir from a key info tuple.
    :param key_info: tuple (key_name, key_versionid, date_lastupdated)
    @return: IssueDir
    """
    key = key_info[0]
    name_no_prefix = key.split("/")[-1]
    canon_name = name_no_prefix.replace("-issue.json", "")
    journal, year, month, day, edition = canon_name.split("-")
    path = key
    issue = IssueDir(journal, date(int(year), int(month), int(day)), edition, path)
    return issue._asdict()


def _key_to_contentitem(key_info):
    """
    Instantiate an ContentItem from a key info tuple.
    :param key_info: tuple (key_name, key_versionid, date_lastupdated)
    @return: ContentItem
    """
    key = key_info[0]  # GDL/1950/01/06/a/GDL-1950-01-06-a-i0056.json
    name_no_prefix = key.split("/")[-1]
    canon_name = name_no_prefix.replace(".json", "")
    journal, year, month, day, edition, number = canon_name.split("-")
    ci_type = number[:1]
    path = key
    return s3ContentItem(
        journal,
        date(int(year), int(month), int(day)),
        edition,
        number[1:],
        path,
        ci_type,
        rebuilt_version=key_info[1],
    )


def _process_keys(key_name, bucket_name, item_type):
    """
    Convert a key in an impresso object: IssueDir or ContentItem
    :param key_name:
    :param bucket_name:
    :param item_type:
    @return:
    """
    # choose the type of build to use
    build = _key_to_issue if item_type == "issue" else _key_to_contentitem
    # retrieve versioning information
    version_id, last_modified = get_s3_versions(bucket_name, key_name)[0]
    # return the object, using the appropriate function
    return build((key_name, version_id, last_modified))


def impresso_iter_bucket(
    bucket_name, item_type=None, prefix=None, filter_config=None, partition_size=15
):
    """
    Iterate over a bucket, possibly with a filter, and return an array of either IssueDir or ContentItem.
    VALID ONLY for original-canonical data, where there is individual files for issues and content items (articles).
    :param bucket_name: string, e.g. 'original-canonical-data'
    :param item_type: 'issue' or 'item'
    :param prefix: string, e.g. 'GDL/1950', used to filter key. Exclusive of 'filter_config'
    :param filter_config: a dict with newspaper acronyms as keys and array of year interval as values:
    e.g. { "GDL": [1950, 1960], "JDG": [1890, 1900] }. Last year is excluded.
    :param partition_size: partition size of dask to build the object (Issuedir or ContentItem)
    @return: an array of (filtered) IssueDir or ContentItems.
    """
    global _WARNED
    if not _WARNED:
        warning = (
            "This function is depreciated and cannot be trusted to yield"
            " correct outputs. Please use s3_iter_bucket instead."
        )
        logger.warning(warning)
        warnings.warn(warning, DeprecationWarning)
    # either prefix or config, but not both
    if prefix and filter_config:
        logger.error("Provide either a prefix or a config but not both")
        return None

    # check which kind of object to build, issue or content_item
    suffix = "issue.json" if item_type == "issue" else ".json"

    # collect keys using pagination
    logger.info(f"Start collecting key from s3 (not parallel)")
    if filter_config is None:
        keys = _list_bucket_paginator(
            bucket_name, prefix, accept_key=lambda key: key.endswith(suffix)
        )
    else:
        keys = _list_bucket_paginator_filter(
            bucket_name,
            accept_key=lambda key: key.endswith(suffix),
            config=filter_config,
        )

    # build IssueDir or ContentItem from the keys, using dask.
    logger.info(f"Start processing key.")
    ci_bag = db.from_sequence(
        keys, partition_size
    )  # default partition_size in dask: about 100
    ci_bag = ci_bag.map(_process_keys, bucket_name=bucket_name, item_type=item_type)
    with ProgressBar():
        result = ci_bag.compute()

    return result


def s3_iter_bucket(bucket_name, prefix, suffix):
    """
    Iterate over a bucket and return all keys with `prefix` and `suffix`.

    >>> b = get_bucket("myBucket", create=False)
    >>> k = s3_iter_bucket(b.name, prefix='GDL', suffix=".bz2")
    >>>
    :param bucket_name: the name of the bucket
    :type bucket_name: str
    :param prefix: beginning of the key
    :type prefix: str
    :param key_suffix: how the key ends
    :type prefix: str
    @return: array of keys
    """
    return _list_bucket_paginator(
        bucket_name, prefix, accept_key=lambda key: key.endswith(suffix)
    )


def s3_filter_archives(bucket_name, config, suffix=".jsonl.bz2"):
    """
    Iterate over bucket and filter according to config and suffix.
    Config is a dict where k= newspaper acronym and v = array of 2 years, considered as time interval.
    Example:
        config = {
            "GDL" : [1960, 1970], => will take all years in interval
            "JDG": [], => Empty array means no filter, all years.
            "GDL": [1798, 1999, 10] => take each 10th item within sequence of years

        }

    :param bucket_name: the name of the bucket
    :type bucket_name: str
    :param config: newspaper/years to consider
    :type config: Dict
    :param key_suffix: end of the key
    :type prefix: str
    :return: array of keys
    """
    filtered_keys = []
    accept_key = lambda k: True
    keynames = ["allyears"]  # todo

    # generate keynames from config (e.g. 'GDL/GDL-1950.jsonl.bz2')
    for np in config:
        if config[np]:
            tmp = [
                np + "/" + np + "-" + str(item) + suffix
                for item in range(config[np][0], config[np][1])
            ]
            if len(config[np]) == 2:
                keynames = tmp
            elif len(config[np]) == 3:
                keynames = tmp[:: config[np][2]]

            accept_key = lambda k: k in keynames

        # retrieving keys
        print(f"Detecting items for {np} with suffixes {keynames}")
        client = get_s3_client()
        paginator = client.get_paginator("list_objects")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=np)
        for page in page_iterator:
            if "Contents" in page:
                for key in page["Contents"]:
                    keyString = key["Key"]
                    if accept_key(keyString):
                        filtered_keys.append(keyString)

    return filtered_keys if filtered_keys else []


def read_s3_issues(newspaper, year, input_bucket):

    def add_version(issue):
        issue["s3_version"] = None
        return issue

    issue_path_ons3 = (
        f"{input_bucket}/{newspaper}/issues/{newspaper}-{year}-issues.jsonl.bz2"
    )
    issues = (
        db.read_text(issue_path_ons3, storage_options=IMPRESSO_STORAGEOPT)
        .map(lambda x: json.loads(x))
        .map(add_version)
        .map(lambda x: (id2IssueDir(x["id"], issue_path_ons3), x))
        .compute()
    )
    return issues


def list_newspapers(
    bucket_name: str,
    s3_client=get_s3_client(),
    page_size: int = 10000,
) -> list[str]:
    """List newspapers contained in an s3 bucket with impresso data.

    Note:
        25,000 seems to be the maximum `PageSize` value supported by
        SwitchEngines' S3 implementation (ceph).
    Note:
        Copied from https://github.com/impresso/impresso-data-sanitycheck/tree/master/sanity_check/contents/s3_data.py

    Args:
        bucket_name (str): Name of the S3 bucket to consider
        s3_client (optional): S3 client to use. Defaults to get_s3_client().
        page_size (int, optional): Pagination configuration. Defaults to 10000.

    Returns:
        list[str]: List of newspaper (aliases) present in the given S3 bucket.
    """
    print(f"Fetching list of newspapers from {bucket_name}")

    if "s3://" in bucket_name:
        bucket_name = bucket_name.replace("s3://", "").split("/")[0]

    paginator = s3_client.get_paginator("list_objects")

    newspapers = set()
    for n, resp in enumerate(
        paginator.paginate(Bucket=bucket_name, PaginationConfig={"PageSize": page_size})
    ):
        # means the bucket is empty
        if "Contents" not in resp:
            continue

        for f in resp["Contents"]:
            newspapers.add(f["Key"].split("/")[0])
        msg = (
            f"Paginated listing of keys in {bucket_name}: page {n + 1}, listed "
            f"{len(resp['Contents'])}"
        )
        logger.info(msg)

    print(f"{bucket_name} contains {len(newspapers)} newspapers")

    return newspapers


def list_files(
    bucket_name: str,
    file_type: str = "issues",
    newspapers_filter: list[str] | None = None,
) -> tuple[list[str] | None, list[str] | None]:
    """List the canonical files located in a given S3 bucket.

    Note:
        adapted from https://github.com/impresso/impresso-data-sanitycheck/tree/master/sanity_check/contents/s3_data.py

    Args:
        bucket_name (str): S3 bucket name.
        file_type (str, optional): Type of files to list, possible values are "issues",
            "pages" and "both". Defaults to "issues".
        newspapers_filter (list[str] | None, optional): List of newspapers to consider.
            If None, all will be considered. Defaults to None.

    Raises:
        NotImplementedError: The given `file_type` is not one of ['issues', 'pages', 'both'].

    Returns:
        tuple[list[str] | None, list[str] | None]: [0] List of issue files or None and
            [1] List of page files or None based on `file_type`
    """
    if file_type not in ["issues", "pages", "both"]:
        logger.error("The provided type is not one of ['issues', 'pages', 'both']!")
        raise NotImplementedError

    # initialize the output lists
    issue_files, page_files = None, None
    # list the newspapers in the bucket
    newspapers = list_newspapers(bucket_name)

    if newspapers_filter is not None:
        suffix = f"for the provided newspapers {newspapers_filter}"
    else:
        suffix = ""

    if file_type in ["issues", "both"]:
        issue_files = [
            file
            for np in newspapers
            if newspapers_filter is not None and np in newspapers_filter
            for file in fixed_s3fs_glob(
                f"{os.path.join(bucket_name, f'{np}/issues/*')}"
            )
        ]
        print(f"{bucket_name} contains {len(issue_files)} .bz2 issue files {suffix}")
    if file_type in ["pages", "both"]:
        page_files = [
            file
            for np in newspapers
            if newspapers_filter is not None and np in newspapers_filter
            for file in fixed_s3fs_glob(f"{os.path.join(bucket_name, f'{np}/pages/*')}")
        ]
        print(f"{bucket_name} contains {len(page_files)} .bz2 page files {suffix}")

    return issue_files, page_files


def fetch_files(
    bucket_name: str,
    compute: bool = True,
    file_type: str = "issues",
    newspapers_filter: list[str] | None = None,
) -> tuple[db.core.Bag | list[str] | None, db.core.Bag | list[str] | None]:
    """Fetch issue and/or page canonical JSON files from an s3 bucket.

    If compute=True, the output will be a list of the contents of all files in the
    bucket for the specified newspapers and type of files.
    If compute=False, the output will remain in a distributed dask.bag.

    Based on file_type, the issue files, page files or both will be returned.
    In the returned tuple, issues are always in the first element and pages in the
    second, hence if file_type is not 'both', the tuple entry corresponding to the
    undesired type of files will be None.

    Note:
        adapted from https://github.com/impresso/impresso-data-sanitycheck/tree/master/sanity_check/contents/s3_data.py

    Args:
        bucket_name (str): Name of the s3 bucket to fetch the files form
        compute (bool, optional): Whether to compute result and output as list.
            Defaults to True.
        file_type: (str, optional): Type of files to list, possible values are "issues",
            "pages" and "both". Defaults to "issues".
        newspapers_filter: (list[str]|None,optional): List of newspapers to consider.
            If None, all will be considered. Defaults to None.

    Raises:
        NotImplementedError: The given `file_type` is not one of ['issues', 'pages', 'both'].

    Returns:
        tuple[db.core.Bag|None, db.core.Bag|None] | tuple[list[str]|None, list[str]|None]:
            [0] Issue files' contents or None and
            [1] Page files' contents or None based on `file_type`
    """
    if file_type not in ["issues", "pages", "both"]:
        logger.error("The provided type is not one of ['issues', 'pages', 'both']!")
        raise NotImplementedError

    issue_files, page_files = list_files(bucket_name, file_type, newspapers_filter)
    # initialize the outputs
    issue_bag, page_bag = None, None

    msg = "Fetching "
    if issue_files is not None:
        msg = f"{msg} issue ids from {len(issue_files)} .bz2 files, "
        issue_bag = db.read_text(issue_files, storage_options=IMPRESSO_STORAGEOPT).map(
            json.loads
        )
    if page_files is not None:
        # make sure all files are .bz2 files and exactly have the naming format they should
        prev_len = len(page_files)
        page_files = [
            p for p in page_files if ".jsonl.bz2" in p and len(p.split("-")) > 5
        ]
        msg = f"{msg} page ids from {len(page_files)} .bz2 files ({prev_len} files before filtering), "
        page_bag = db.read_text(page_files, storage_options=IMPRESSO_STORAGEOPT).map(
            json.loads
        )

    logger.info(msg)

    if compute:
        page_bag = page_bag.compute() if page_files is not None else page_bag
        issue_bag = issue_bag.compute() if issue_files is not None else issue_bag

    return issue_bag, page_bag
