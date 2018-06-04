"""Code for parsing impresso's canonical directory structures."""

import os
import logging
from datetime import date, datetime
from smart_open import s3_iter_bucket
from collections import namedtuple
import re
import json

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

ContentItem = namedtuple(
    "Item", [
        'journal',
        'date',
        'edition',
        'number',
        'path',
        'type'
    ]
)


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


def s3_detect_issues(input_bucket, prefix=None):
    """
    Detect all issues stored in an S3 drive/bucket.

    The path in `issue.path` is the S3 key name.

    @param input_bucket: name of the bucket to consider
    @param prefix: prefix to consider (e.g. 'GDL' or 'GDL/1910')
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

    if prefix is None:
        return [
            _key_to_issue(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                accept_key=lambda key: key.endswith('issue.json')
            )
        ]
    else:
        return [
            _key_to_issue(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                prefix=prefix,
                accept_key=lambda key: key.endswith('issue.json')
            )
        ]


def s3_detect_contentitems(input_bucket, prefix=None):
    """
    Detect all content_items stored in an S3 drive/bucket.

    The path in `contentitem.path` is the S3 key name.

    @param input_bucket: name of the bucket to consider
    @param prefix: prefix to consider (e.g. 'GDL' or 'GDL/1910')
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

    if prefix is None:
        return [
            _key_to_contentitem(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                accept_key=lambda key: key.endswith('.json')
            )
        ]
    else:
        return [
            _key_to_contentitem(key)
            for key, content in s3_iter_bucket(
                input_bucket,
                prefix=prefix,
                accept_key=lambda key: key.endswith('.json')
            )
        ]


def s3_select_issues(input_bucket, np_config):
    """
    Select issues stored in an S3 drive/bucket.

    @param input_bucket: the name of the bucket
    @param newspaper_config: a json file specifying the selection where
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

    for np in np_config:
        if np_config[np]:
            k = []
            prefixes = [np + "/" + str(item) for item in range(np_config[np][0], np_config[np][1])]
            logger.info(f"Detecting issues for {np} for years {prefixes}")
            for prefix in prefixes:
                t = [
                    _key_to_issue(key)
                    for key, content in s3_iter_bucket(
                        input_bucket,
                        prefix=prefix,
                        accept_key=lambda key: key.endswith('issue.json')
                    )
                ]
                k.extend(t)
        else:
            k = [
                _key_to_issue(key)
                for key, content in s3_iter_bucket(
                    input_bucket,
                    prefix=np,
                    accept_key=lambda key: key.endswith('issue.json')
                )
            ]
        keys.extend(k)
    return keys


def s3_select_contentitems(input_bucket, np_config):
    """
    Select content_items (i.e. articles or pages) stored in an S3 drive/bucket.

    @param input_bucket: the name of the bucket
    @param newspaper_config: a json file specifying the selection where
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
            ci_type
        )

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
                        accept_key=lambda key: key.endswith('.json')
                    )
                ]
                k.extend(t)
        else:
            k = [
                _key_to_contentitem(key)
                for key, content in s3_iter_bucket(
                    input_bucket,
                    prefix=np,
                    accept_key=lambda key: key.endswith('.json')
                )
            ]
        keys.extend(k)
    return keys
