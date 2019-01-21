"""TODO"""

import json
import logging
import os

from impresso_commons.utils.s3 import get_s3_versions, get_s3_resource

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
    """Read all pages of a given issue from S3 in parallel.

    :param issue: input issue
    :type issue: `IssueDir`
    :param issue_json: a JSON canonical issue
    :type issue_json: dict
    :param bucket: input bucket's name
    :type bucket: str
    :return: a list of tuples: [0] `IssueDir`, [1] JSON canonical issue
    :rytpe: list of tuples
    """

    pages = []
    # create one S3 connection and use it for all pages in the issue
    s3 = get_s3_resource()

    # reconstruct the page key from an IssueDir object and the list of int
    # contained in `issue_json['pp']`
    for page in issue_json["pp"]:
        page_key = os.path.join(
            "/".join(issue.path.split('/')[:-1]),
            f"{page}.json"
        )
        pages.append(read_page(page_key, bucket_name=bucket, s3_client=s3))
    issue_json['pp'] = pages
    return (issue, issue_json)


def rejoin_articles(issue, issue_json):
    logger.info(f"Rejoining pages for issue {issue.path}")

    articles = []
    for article in issue_json['i']:

        article['m']['s3v'] = issue_json['s3_version']

        pages = []
        for page_no in article['m']['pp']:
            # given a page  number (from issue.json) and its canonical ID
            # find the position of that page in the array of pages (with text
            # regions)
            page_no_string = f"p{str(page_no).zfill(4)}"
            page_idx = [
                n
                for n, page in enumerate(issue_json['pp'])
                if page_no_string in page['id']
            ][0]
            pages.append(issue_json['pp'][page_idx])

        articles.append((article, pages))
    return articles


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
