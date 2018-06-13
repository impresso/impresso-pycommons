"""Code for parsing impresso's canonical directory structures."""

import os
import logging
from datetime import date, datetime
from smart_open import s3_iter_bucket
from collections import namedtuple
from impresso_commons.utils.s3 import get_s3_versions
import re
import json

from impresso_commons.utils import _get_cores

logger = logging.getLogger(__name__)

# a simple data structure to represent input directories
# a `Document.zip` file is expected to be found in `IssueDir.path`
IssueDir = namedtuple(
    "IssueDirectory", [
        'journal',
        'date',
        'edition',
        'path'
    ]
)

# ContentItem = namedtuple(  # Todo: add version
#     "Item", [
#         'journal',
#         'date',
#         'edition',
#         'number',
#         'path',
#         'type',
#         'rebuilt_version',
#         'canonical_version'
#     ]
# )


class ContentItem:
    def __init__(self, journal, date, edition, number, path, type=None, rebuilt_version=None, canonical_version=None):
        self.journal = journal
        self.date = date
        self.edition = edition
        self.number = number
        self.path = path
        self.type = None
        self.rebuilt_version = rebuilt_version
        self.canonical_version = canonical_version


KNOWN_JOURNALS = [
    "BDC",
    "CDV",
    "DLE",
    "EDA",
    "EXP",
    "IMP",
    "GDL",
    "JDF",
    "LBP",
    "LCE",
    "LCG",
    "LCR",
    "LCS",
    "LES",
    "LNF",
    "LSE",
    "LSR",
    "LTF",
    "LVE",
    "EVT",
    "JDG",
    "LNQ",
]


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
        return ContentItem(
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
        return ContentItem(
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



