"""Reusable functions to read/write data from/to our S3 drive."""

import os

import json
import boto
import boto.s3.connection
from smart_open import s3_iter_bucket


def get_s3_connection(host="os.zhdk.cloud.switch.ch"):
    """Create a connection to impresso's S3 drive.

    Assumes that two environment variables are set: `SE_ACCESS_KEY` and
        `SE_SECRET_KEY`.
    """
    try:
        access_key = os.environ["SE_ACCESS_KEY"]
    except Exception:
        raise

    try:
        secret_key = os.environ["SE_SECRET_KEY"]
    except Exception:
        raise

    return boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host=host,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
    )


def s3_get_articles(issue, bucket):
    """Read a newspaper issue from S3 and return the articles it contains.

    :param issue: the newspaper issue
    :type issue: an instance of `impresso_commons.path.IssueDir`
    :param bucket: the input s3 bucket
    :type bucket: `boto.s3.bucket import Bucket`
    :return: a list of articles (dictionaries)

    NB: Content items with type = "ad" (advertisement) are filtered out.
    """
    issue_data = list(s3_iter_bucket(bucket, prefix=issue.path))[0][1]
    issue_json = json.loads(issue_data.decode('utf-8'))
    articles = [
        item
        for item in issue_json["i"]
        if item["m"]["tp"] == "article"]
    return articles


def s3_get_pages(issue_id, page_names, bucket):
    """Read in canonical text data for all pages in a given newspaper issue.

    :param issue_id: the canonical issue id (e.g. "IMP-1990-03-15-a")
    :type issue_id: string
    :param page_names: a list of canonical page filenames
        (e.g. "IMP-1990-03-15-a-p0001.json")
    :type page_names: list of strings
    :param bucket: the s3 bucket where the pages to be read are stored
    :type bucket: instance of `boto.Bucket`
    :return: a dictionary with page filenames as keys, and JSON data as values.
    """
    return {
        key.name.split('/')[-1]: json.loads(content.decode('utf-8'))
        for key, content in s3_iter_bucket(
            bucket,
            prefix=issue_id.replace('-', '/')
        )
        if key.name.split('/')[-1] in list(page_names.values())
    }
