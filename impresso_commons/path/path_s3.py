"""Code for parsing impresso's canonical directory structures."""

import logging
from datetime import date
from smart_open import s3_iter_bucket
from collections import namedtuple
from deprecated import deprecated

from dask.diagnostics import ProgressBar
import dask.bag as db

from impresso_commons.utils import _get_cores
from impresso_commons.utils.s3 import get_s3_client, get_s3_versions

logger = logging.getLogger(__name__)


# a simple data structure to represent input directories
IssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path'
    ]
)


class s3ContentItem:
    def __init__(self, journal, date, edition, number, key_name,
                 doc_type=None, rebuilt_version=None, canonical_version=None):
        self.journal = journal
        self.date = date
        self.edition = edition
        self.number = number
        self.doc_type = doc_type
        self.key_name = key_name
        self.rebuilt_version = rebuilt_version
        self.canonical_version = canonical_version


def s3_detect_issues(input_bucket, prefix=None, workers=None):
    """
    Detect all issues stored in an S3 drive/bucket.

    The path in `issue.path` is the S3 key name.

    @param input_bucket: name of the bucket to consider
    @param prefix: prefix to consider (e.g. 'GDL' or 'GDL/1910')
    @param workers: number of workers for the s3_iter_bucket function. If None, will be the number of detected CPUs.
    @return: a list of `IssueDir` instances.
    """

    def _key_to_issue(key):
        """Instantiate an IssueDir from a (canonical) key name."""
        name_no_prefix = key.name.split('/')[-1]
        canon_name = name_no_prefix.replace("-issue.json", "")
        journal, year, month, day, edition = canon_name.split('-')
        path = key.name
        return IssueDir(
            journal,
            date(int(year), int(month), int(day)),
            edition,
            path
        )

    nb_workers = _get_cores() if workers is None else workers

    if prefix is None:
        return [
            _key_to_issue(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                accept_key=lambda key: key.endswith('issue.json'),
                workers=_get_cores()
            )
        ]
    else:
        return [
            _key_to_issue(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                prefix=prefix,
                accept_key=lambda key: key.endswith('issue.json'),
                workers=_get_cores()
            )
        ]


@deprecated(reason="smart_open needlessly -for us- downloads the content of the key. Prefer impresso_s3_iter_bucket")
def s3_detect_contentitems(input_bucket, prefix=None, workers=None):
    """
    Detect all content_items stored in an S3 drive/bucket.

    The path in `contentitem.path` is the S3 key name.

    @param input_bucket: name of the bucket to consider
    @param prefix: prefix to consider (e.g. 'GDL' or 'GDL/1910')
    @param workers: number of workers for the s3_iter_bucket function. If None, will be the number of detected CPUs.
    @return:a list of `ContentItem` instances.
    """

    def _key_to_contentitem(key):  # GDL-1910-01-10-a-i0002.json
        """Instantiate an ContentItem from a (canonical) key name."""
        name_no_prefix = key.name.split('/')[-1]
        canon_name = name_no_prefix.replace(".json", "")
        journal, year, month, day, edition, number = canon_name.split('-')
        ci_type = number[:1]
        path = key.name
        return s3ContentItem(
            journal,
            date(int(year), int(month), int(day)),
            edition,
            number[1:],
            path,
            ci_type
        )

    nb_workers = _get_cores() if workers is None else workers

    if prefix is None:
        return [
            _key_to_contentitem(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                accept_key=lambda key: key.endswith('.json'),
                workers=_get_cores()
            )
        ]
    else:
        return [
            _key_to_contentitem(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                prefix=prefix,
                accept_key=lambda key: key.endswith('.json'),
                workers=_get_cores()
            )
        ]


@deprecated(reason="smart_open needlessly -for us- downloads the content of the key. Prefer impresso_s3_iter_bucket")
def s3_select_issues(input_bucket, np_config, workers=None):
    """
    Select issues stored in an S3 drive/bucket.

    @param input_bucket: the name of the bucket
    @param newspaper_config: a json file specifying the selection where
    @param workers: number of workers for the s3_iter_bucket function. If None, will be the number of detected CPUs.
    [keys = newspaper acronym and values = array]
    The value array can either contains 2 dates (start and end of interval to consider)
    or be empty (all years will be considered).
    Ex:
        config = {
              "GDL" : [1910,1915],
              "BDC" : []
            }

    @return:  a list of `IssueDir` instances.
    """

    keys = []

    def _key_to_issue(key):
        """Instantiate an IssueDir from a (canonical) key name."""
        name_no_prefix = key.name.split('/')[-1]
        canon_name = name_no_prefix.replace("-issue.json", "")
        journal, year, month, day, edition = canon_name.split('-')
        path = key.name
        return IssueDir(
            journal,
            date(int(year), int(month), int(day)),
            edition,
            path
        )

    nb_workers = _get_cores() if workers is None else workers

    logger.info(f"Start selecting issues with {nb_workers} for {np_config}")
    for np in np_config:
        if np_config[np]:
            k = []
            prefixes = [np + "/" + str(item) for item in range(np_config[np][0], np_config[np][1])]
            logger.info(f"Selecting issues for {np} for years {prefixes}")
            for prefix in prefixes:
                t = [
                    _key_to_issue(key)
                    for key, content in s3_iter_bucket(
                        input_bucket,
                        prefix=prefix,
                        accept_key=lambda key: key.endswith('issue.json'),
                        workers=_get_cores()
                    )
                ]
                k.extend(t)
        else:
            k = [
                _key_to_issue(key)
                for key, content in s3_iter_bucket(
                    input_bucket,
                    prefix=np,
                    accept_key=lambda key: key.endswith('issue.json'),
                    workers=_get_cores()
                )
            ]
        keys.extend(k)
    return keys


@deprecated(reason="smart_open needlessly -for us- downloads the content of the key. Prefer impresso_s3_iter_bucket ")
def s3_select_contentitems(input_bucket, np_config, workers=None):
    """
    Select content_items (i.e. articles or pages) stored in an S3 drive/bucket.

    @param input_bucket: the name of the bucket
    @param newspaper_config: a json file specifying the selection where
    @param workers: number of workers for the s3_iter_bucket function. If None, will be the number of detected CPUs.
    [keys = newspaper acronym and values = array]
    The value array can either contains 2 dates (start and end of interval to consider)
    or be empty (all years will be considered).
    Ex:
        config = {
              "GDL" : [1910,1915],
              "BDC" : []
            }

    @return:  a list of `ContentItem` instances.

    """
    keys = []

    def _key_to_contentitem(key):  # GDL-1910-01-10-a-i0002.json
        """Instantiate an ContentItem from a (canonical) key name."""
        name_no_prefix = key.name.split('/')[-1]
        canon_name = name_no_prefix.replace(".json", "")
        journal, year, month, day, edition, number = canon_name.split('-')
        ci_type = number[:1]
        path = key.name
        return s3ContentItem(
            journal,
            date(int(year), int(month), int(day)),
            edition,
            number[1:],
            path,
            ci_type,
        )

    nb_workers = _get_cores() if workers is None else workers

    logger.info(f"Start selecting content items with {nb_workers} for {np_config}")
    for np in np_config:
        if np_config[np]:
            k = []
            prefixes = [np + "/" + str(item) for item in range(np_config[np][0], np_config[np][1])]
            logger.info(f"Detecting content items for {np} for years {prefixes}")
            for prefix in prefixes:
                t = [
                    _key_to_contentitem(key)
                    for key, content in s3_iter_bucket(
                        input_bucket,
                        prefix=prefix,
                        accept_key=lambda key: key.endswith('.json'),
                        workers=nb_workers
                    )
                ]
                k.extend(t)
        else:
            k = [
                _key_to_contentitem(key)
                for key, content in s3_iter_bucket(
                    input_bucket,
                    prefix=np,
                    accept_key=lambda key: key.endswith('.json'),
                    workers=nb_workers
                )
            ]
        keys.extend(k)
    return keys


def _list_bucket_paginator(bucket_name, prefix='', accept_key=lambda k: True):
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


def _list_bucket_paginator_filter(bucket_name, prefix='', accept_key=lambda k: True, config=None):
    if config is None:
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

    else:
        for np in config:
            # if years are specified, take the range
            if config[np]:
                prefixes = [np + "/" + str(item) for item in range(config[np][0], config[np][1])]
            # otherwise prefix is just the newspaper
            else:
                prefixes = [np]
            print(f"Detecting items for {np} for years {prefixes}")
            filtered_keys = []
            for prefix in prefixes:
                client = get_s3_client()
                paginator = client.get_paginator("list_objects")
                page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
                keys = []
                for page in page_iterator:
                    if "Contents" in page:
                        for key in page["Contents"]:
                            keyString = key["Key"]
                            if accept_key(keyString):
                                filtered_keys.append(keyString)
            return filtered_keys if filtered_keys else []


def _key_to_issue(key_info):
    """Instantiate an IssueDir from a key info tuple."""
    key = key_info[0]
    name_no_prefix = key.split('/')[-1]
    canon_name = name_no_prefix.replace("-issue.json", "")
    journal, year, month, day, edition = canon_name.split('-')
    path = key
    issue = IssueDir(
        journal,
        date(int(year), int(month), int(day)),
        edition,
        path
    )
    return issue._asdict()


def _key_to_contentitem(key_info):
    """
    Instantiate an ContentItem from a key info tuple.
    @param key_info: tuple (key_name, key_versionid, date_lastupdated)
    @return: ContentItem
    """
    key = key_info[0]  # GDL/1950/01/06/a/GDL-1950-01-06-a-i0056.json
    name_no_prefix = key.split('/')[-1]
    canon_name = name_no_prefix.replace(".json", "")
    journal, year, month, day, edition, number = canon_name.split('-')
    ci_type = number[:1]
    path = key
    return s3ContentItem(
        journal,
        date(int(year), int(month), int(day)),
        edition,
        number[1:],
        path,
        ci_type,
        rebuilt_version=key_info[1]
    )


def _process_keys(key_name, bucket_name, item_type):
    build = _key_to_issue if item_type == "issue" else _key_to_contentitem
    version_id, last_modified = get_s3_versions(bucket_name, key_name)[0]
    return build((key_name, version_id, last_modified))


def impresso_iter_bucket(bucket_name,
                         item_type=None,
                         prefix=None,
                         filter_config=None,
                         partition_size=15):
    # either prefix or config, but not both
    if prefix and filter_config:
        logger.error("Provide either a prefix or a config but not both")
        return None

    # check which kind of object to build, issue or content_item
    suffix = 'issue.json' if item_type == "issue" else '.json'

    logger.info(f"Start collecting key from s3 (not parallel)")
    if filter_config is None:
        keys = _list_bucket_paginator(bucket_name, prefix, accept_key=lambda key: key.endswith(suffix))
    else:
        keys = _list_bucket_paginator_filter(bucket_name, accept_key=lambda key: key.endswith(suffix),
                                             config=filter_config)

    logger.info(f"Start processing key.")
    ci_bag = db.from_sequence(keys, partition_size)
    ci_bag = ci_bag.map(_process_keys, bucket_name=bucket_name, item_type=item_type)

    with ProgressBar():
        result = ci_bag.compute()
    return result
