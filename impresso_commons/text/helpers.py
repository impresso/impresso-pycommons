"""TODO"""

import json
import logging
import os

from dask import bag as db

from impresso_commons.utils.s3 import (IMPRESSO_STORAGEOPT,
                                       alternative_read_text, get_s3_resource,
                                       get_s3_versions)

logger = logging.getLogger(__name__)


def read_issue(issue, bucket_name, s3_client=None):
    """Read the data from S3 for a given newspaper issue.

    NB: It injects the s3_version into the returned object.

    :param issue: input issue
    :type issue: `IssueDir`
    :param bucket_name: bucket's name
    :type bucket_name: str
    :param s3_client: open connection to S3 storage
    :type s3_client: `boto3.resources.factory.s3.ServiceResource`
    :return: a JSON representation of the issue object
    """
    if s3_client is None:
        s3_client = get_s3_resource()

    content_object = s3_client.Object(bucket_name, issue.path)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    issue_json = json.loads(file_content)
    issue_json["s3_version"] = get_s3_versions(bucket_name, issue.path)[0][0]
    logger.info("Read JSON of {}".format(issue))
    return (issue, issue_json)


def read_page(page_key, bucket_name, s3_client):
    """Read the data from S3 for a given newspaper pages."""

    try:
        content_object = s3_client.Object(bucket_name, page_key)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        page_json = json.loads(file_content)
        page_json["s3v"] = get_s3_versions(bucket_name, page_key)[0][0]
        logger.info("Read page {} from bucket {}".format(
            page_key,
            bucket_name
        ))
        return page_json
    except Exception as e:
        logger.error(f'There was a problem reading {page_key}: {e}')
        return None


def read_issue_pages(issue, issue_json, bucket=None):
    """Read all pages of a given issue from S3 in parallel."""
    newspaper = issue.journal
    year = issue.date.year

    filename = (
            f"{bucket}/{newspaper}/pages/{newspaper}-{year}"
            f"/{issue_json['id']}-pages.jsonl.bz2"
    )

    pages = [
        json.loads(page)
        for page in alternative_read_text(filename, IMPRESSO_STORAGEOPT)
    ]

    """
    pages = db.read_text(
        filename,
        storage_options=IMPRESSO_STORAGEOPT
    ).map(lambda x: json.loads(x)).compute()
    """
    print(filename)
    issue_json["pp"] = pages
    del pages
    return (issue, issue_json)


def rejoin_articles(issue, issue_json):
    print(f"Rejoining pages for issue {issue.path}")
    articles = []
    for article in issue_json['i']:

        art_id = article['m']['id']
        article['m']['s3v'] = issue_json['s3_version']
        article['has_problem'] = False
        article['m']['pp'] = sorted(list(set(article['m']['pp'])))

        pages = []
        page_ids = [
            page['id']
            for page in issue_json['pp']
        ]
        for page_no in article['m']['pp']:
            # given a page  number (from issue.json) and its canonical ID
            # find the position of that page in the array of pages (with text
            # regions)
            page_no_string = f"p{str(page_no).zfill(4)}"
            try:
                page_idx = [
                    n
                    for n, page in enumerate(issue_json['pp'])
                    if page_no_string in page['id']
                ][0]
                pages.append(issue_json['pp'][page_idx])
            except IndexError:
                article['has_problem'] = True
                articles.append(article)
                logger.error(
                    f'Page {page_no_string} not found for item {art_id}'
                    f"Issue {issue_json['id']} has pages {page_ids}"
                )
                continue

        regions_by_page = []
        for page in pages:
            regions_by_page.append([
                region
                for region in page["r"]
                if "pOf" in region and region["pOf"] == art_id
            ])
        article['pprr'] = regions_by_page
        try:
            convert_coords = [p['cc'] for p in pages]
            article['m']['cc'] = sum(convert_coords) / len(convert_coords) == 1.0
        except Exception:
            # it just means there was no CC field in the pages
            article['m']['cc'] = None

        articles.append(article)
    return articles


def pages_to_article(article, pages):
    """Return all text regions belonging to a given article."""
    try:
        art_id = article['m']['id']
        print("Extracting text regions for article {}".format(art_id))
        regions_by_page = []
        for page in pages:
            regions_by_page.append([
                region
                for region in page["r"]
                if region["pOf"] == art_id
            ])
        convert_coords = [page['cc'] for page in pages]
        article['m']['cc'] = sum(convert_coords) / len(convert_coords) == 1.0
        article['has_problem'] = False
        article['pprr'] = regions_by_page
        return article
    except Exception as e:
        article['has_problem'] = True
        return article


def text_apply_breaks(fulltext, breaks):
    """Apply breaks to the text returned by `rebuild_for_solr`.

    The purpose of this function is to debug (visually) the `rebuild_for_solr`
    function. It applies to `fulltext` the characte offsets contained in
    `breaks` (e.g. line breaks, paragraph breaks, etc.).

    :param fulltext: input text
    :type fulltext: str
    :param breaks: a list of character offsets
    :type breaks: list of int
    :return: a list of text chunks
    :rtype: list
    """

    text = []
    start = 0

    for br in breaks:
        text.append(fulltext[start:br].strip())
        start = br

    text.append(fulltext[start:])

    return text
