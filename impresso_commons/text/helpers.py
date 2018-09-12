"""TODO"""

import json
import logging
import os

import dask.bag as db

from impresso_commons.utils.s3 import get_s3_versions

logger = logging.getLogger(__name__)


def read_issue(issue, bucket):
    """Read the data from S3 for a given newspaper issue.

    NB: It injects the s3_version into the returned object.

    :param issue: input issue
    :type issue: `IssueDir`
    :return: a JSON representation of the issue object
    """
    k = bucket.get_key(issue.path)
    content = k.get_contents_as_string(encoding="utf-8")
    issue_json = json.loads(content)
    issue_json["s3_version"] = get_s3_versions(
            bucket.name,
            k.name
    )[0][0]
    logger.info("Read JSON of {}".format(issue))
    return (issue, issue_json)


def read_page(page_key, bucket):
    """Read the data from S3 for a given newspaper pages."""

    try:
        k = bucket.get_key(page_key)
        content = k.get_contents_as_string(encoding="utf-8")
        page_json = json.loads(content)
        page_json["id"] = page_key.split("/")[-1]
        page_json["s3_version"] = get_s3_versions(bucket.name, page_key)[0][0]
        logger.info("Read page {} from bucket {}".format(
            page_key,
            bucket.name
        ))
        k.close()
        return page_json
    except Exception as e:
        logger.error(f'There was a problem reading {page_key}: {e}')
        return None


def read_issue_pages(issue, issue_json, bucket=None):
    """Read all pages of a given issue from S3 in parallel."""

    pages = []

    for page in issue_json["pp"]:
        page_key = os.path.join(
            "/".join(issue.path.split('/')[:-1]),
            f"{page}.json"
        )
        pages.append(
            read_page(page_key, bucket=bucket)
        )
    issue_json['pp'] = pages
    return (issue, issue_json)


def rejoin_articles(issue, issue_json):
    logger.info(f"Rejoining pages for issue {issue.path}")
    return [
        (
            article,
            [
                issue_json['pp'][page_no - 1]
                for page_no in article['m']['pp']
            ]
        )
        for article in issue_json['i']
        if article['m']['tp'] != 'ad'
    ]


def pages_to_article(article, pages):
    """Return all text regions belonging to a given article."""
    try:
        art_id = article['m']['id']
        logger.info("Extracting text regions for article {}".format(art_id))
        regions_by_page = []
        for page in pages:
            regions_by_page.append([
                region
                for region in page["r"]
                if region["pOf"] == art_id
            ])
        article['has_problem'] = False
        article['pprr'] = regions_by_page
        return article
    except Exception as e:
        article['has_problem'] = True
        return article
