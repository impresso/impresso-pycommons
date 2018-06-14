"""Code for parsing impresso's canonical directory structures."""

import logging
from datetime import date
from smart_open import s3_iter_bucket
from collections import namedtuple
from deprecated import deprecated
import multiprocessing
import contextlib
import six

from impresso_commons.utils import _get_cores
from impresso_commons.utils.s3 import get_s3_client, get_s3_versions

logger = logging.getLogger(__name__)

_MULTIPROCESSING = False
try:
    import multiprocessing.pool
    _MULTIPROCESSING = True
except ImportError:
    print("multiprocessing could not be imported and won't be used")


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


@deprecated(reason="smart_open needlessly -for us- downloads the content of the key. Prefer ")
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


@deprecated(reason="smart_open needlessly -for us- downloads the content of the key. Prefer ")
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


def impresso_s3_iter_bucket(
        bucket_name,
        item_type=None,
        prefix=None,
        filter_config=None,
        key_limit=None,
        workers=16,
        retries=3,
        ):
    """
    Adapted from smartopen https://github.com/RaRe-Technologies/smart_open/blob/master/smart_open/s3.py
    for impresso needs

    Iterate and download all S3 files under `bucket/prefix`, yielding out an item generator (ContentItem or Issue).

    `accept_key` is a function that accepts a key name (unicode string) and
    returns True/False, signalling whether the given key should be downloaded out or
    not (default: accept all keys).

    If `key_limit` is given, stop after yielding out that many results.
    The keys are processed in parallel, using `workers` processes (default: 16),
    to speed up downloads greatly. If multiprocessing is not available, thus
    _MULTIPROCESSING is False, this parameter will be ignored.

    You can specify a json config such as :
        >>> np_config = {
        >>> "GDL" : [1950, 1952], # => start / end of year interval to consider
        >>> "BDC" : [] # => all years are considered
        >>> }


    Example::
      >>> # get all JSON files under "GDL/1950/"
      >>> for key, content in iter_bucket(bucket_name, prefix="GDL/1950/", accept_key=lambda key: key.endswith('.json')):
      ...     print key, len(content)
      >>> # limit to 10k files, using 32 parallel workers (default is 16)
      >>> for key, content in iter_bucket(bucket_name, key_limit=10000, workers=32):
      ...     print key, len(content)

    @param bucket_name: the bucket string name
    @param prefix: the prefix to select keys from. if defined, config must be None.
    @param accept_key: see explanation above
    @param key_limit: id.
    @param workers: id.
    @param retries: id.
    @param config: a json object holding newspapers as keys and array of years as values
    @return:
    """

    # if bucket instance, silently extract the name
    try:
        bucket_name = bucket_name.name
    except AttributeError:
        pass

    # either prefix or config, but not both
    if prefix and filter_config:
        logger.error("Provide either a prefix or a config but not both")
        return None

    # check which kind of object to build, issue or content_item
    if item_type == "issue":
        suffix = 'issue.json'
        build = _key_to_issue
    elif item_type == "content_item":
        suffix = '.json'
        build = _key_to_contentitem
    else:
        logger.error("Specify the type of item to retrieve from S3.")
        return None

    # get the key iterator - if specified, filter is applied
    key_no = -1
    key_iterator = _list_bucket(bucket_name, prefix=prefix,
                                accept_key=lambda key: key.endswith(suffix), config=filter_config)

    # create objects in parallel
    with _create_process_pool(processes=workers) as pool:
        result_iterator = pool.imap_unordered(build, key_iterator)
        for key_no, item in enumerate(result_iterator):
            if key_no % 1000 == 0:

                logger.info(key_no)

                logger.info(
                    "yielding key #%i: %s",
                    key_no, item
                )

                yield item

            if key_limit is not None and key_no + 1 >= key_limit:
                # we were asked to output only a limited number of keys => we're done
                break
    logger.info(f"processed {key_no} keys")


def _list_bucket(bucket_name, prefix='', accept_key=lambda k: True, config=None):
    """
    Adapted from smartopen https://github.com/RaRe-Technologies/smart_open/blob/master/smart_open/s3.py

    Iterate 'bucket_name' to retrieve keys filtered by `prefix` of newspaper/range of years of 'config'.

    Return a (key, version_id, last_modified) 3-tuple generator.

    @param bucket_name:
    @param prefix:
    @param accept_key:
    @param config:
    @return:
    """

    client = get_s3_client()

    if config is None:
        ctoken = None
        while True:
            response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            try:
                content = response['Contents']
            except KeyError:
                pass
            else:
                for c in content:
                    key = c['Key']
                    if accept_key(key):
                        version_id, last_modified = get_s3_versions(bucket_name, key)[0]
                        yield (key, version_id, last_modified)
            ctoken = response.get('NextContinuationToken', None)
            if not ctoken:
                break
    else:
        for np in config:
            # if years are specified, take the range
            if config[np]:
                prefixes = [np + "/" + str(item) for item in range(config[np][0], config[np][1])]
            # otherwise prefix os just the newspaper
            else:
                prefixes = [np]
            print(f"Detecting items for {np} for years {prefixes}")
            for prefix in prefixes:
                ctoken = None
                while True:
                    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                    try:
                        content = response['Contents']
                    except KeyError:
                        pass
                    else:
                        for c in content:
                            key = c['Key']
                            if accept_key(key):
                                version_id, last_modified = get_s3_versions(bucket_name, key)[0]
                                yield (key, version_id, last_modified)
                    ctoken = response.get('NextContinuationToken', None)
                    if not ctoken:
                        break


def convert(obj):  # todo: strangly does not work when call on issue
    """
    To convert a dictionary into a IssueDir namedtuple.
    @param obj:
    @return:
    """
    for key, value in obj.items():
        obj[key] = convert(value)
    return namedtuple('IssueDir', obj.keys())(**obj)


class DummyPool(object):
    """A class that mimics multiprocessing.pool.Pool for our purposes."""
    def imap_unordered(self, function, items):
        return six.moves.map(function, items)

    def terminate(self):
        pass


@contextlib.contextmanager
def _create_process_pool(processes=1):
    if _MULTIPROCESSING and processes:
        print("creating pool with %i workers", processes)
        pool = multiprocessing.pool.Pool(processes=processes)
    else:
        print("creating dummy pool")
        pool = DummyPool()
    yield pool
    pool.terminate()



