"""Functions and CLI to rebuild text from impresso's canonical format.

Usage:
    rebuilder.py rebuild_articles --input-bucket=<b> --log-file=<f> (--output-dir=<od> | --output-bucket=<ob>) [--verbose --clear --format=<f>]
    rebuilder.py rebuild_pages --input-bucket=<b> --log-file=<f> (--output-dir=<od> | --output-bucket=<ob>) [--verbose --clear --format=<f>]
"""  # noqa: E501

import codecs
import datetime
import json
import logging
import os
import shutil
from functools import reduce
from itertools import starmap

import dask
import dask.bag as db
import ipdb as pdb  # remove from production version
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from dask.distributed import Client, progress
from docopt import docopt

from impresso_commons.text.helpers import read_issue, read_issue_pages
from impresso_commons.path.path_fs import IssueDir, detect_issues
from impresso_commons.path.path_s3 import (impresso_iter_bucket,
                                           s3_detect_issues)
from impresso_commons.utils.s3 import (get_bucket, get_s3_connection,
                                       get_s3_versions, s3_get_pages)

dask.set_options(get=dask.threaded.get)


logger = logging.getLogger('impresso_commons')


# TODO: transform into `serialize_by_year` then abandon
def serialize_article(article, output_format, output_dir):
    """Write the rebuilt article to disk in a given format (json or text).

    :param article: the output of `rebuild_article()`
    :type article: dict
    :param output_format: text or json (for now)
    :type output_format: string
    :param outp_dir:
    :type output_dir: string
    """

    if output_format == "json":
        out_filename = os.path.join(output_dir, f"{article['id']}.json")

        with codecs.open(out_filename, "w", "utf-8") as out_file:
            json.dump(article, out_file, indent=3)
            logger.info("serialized {}".format(article["id"]))

    elif output_format == "txt":
        fulltext = article.pop("text")

        out_filename = os.path.join(output_dir, f"{article['id']}.json")
        with codecs.open(out_filename, "w", "utf-8") as out_file:
            json.dump(article, out_file, indent=3)
            logger.info("serialized metadata for {}".format(article["id"]))

        out_filename = os.path.join(output_dir, f"{article['id']}.txt")
        with codecs.open(out_filename, "w", "utf-8") as out_file:
            out_file.write(fulltext)
            logger.info("serialized fulltext for {}".format(article["id"]))

    else:
        raise Exception("Unsupported output format {}".format(output_format))

    return


def rebuild_text(tokens, string=None):
    """The text rebuilding function."""

    regions = []

    if string is None:
        string = ""

    for token in tokens:
        region = {}
        region["coords"] = token["c"]
        region["start"] = len(string)
        region["length"] = len(token["tx"])
        string += "{} ".format(token["tx"])  # TODO: check `if "gn" in token`
        regions.append(region)
        logger.debug(token["tx"])

    return (string, regions)


# TODO: reuse part the of the logic for `helpers.pages_to_article()`
# then abandon
def rebuild_article(article_metadata, bucket, output="JSON"):
    """Rebuilds the text of an article given its metadata as input.

    It fetches the JSON for each of its pages from S3, and applies the text
    rebuilding function corresponding to article's language.

    :param article_metadata: the article's metadata
    :type article_metadata: dict
    :param bucket: the s3 bucket where the pages are stored
    :type bucket: `boto.s3.bucket.Bucket`
    :return: a dictionary with the following keys: "id", "pages", "text",
        "datetime", "title", "lang", "journal"
    :rtype: dict
    """
    article_id = article_metadata["m"]["id"]
    logger.info(f'Started rebuilding article {article_id}')
    issue_id = "-".join(article_id.split('-')[:-1])
    page_file_names = {
        p: "{}-p{}.json".format(issue_id, str(p).zfill(4))
        for p in article_metadata["m"]["pp"]
    }
    pages = s3_get_pages(issue_id, page_file_names, bucket)
    year, month, day = article_id.split('-')[1:4]
    d = datetime.datetime(int(year), int(month), int(day), 5, 0, 0)

    fulltext = ""
    article = {
        "id": article_id,
        # "series": None,
        "pages": [],
        "datetime": d.isoformat() + 'Z',
        "title": article_metadata["m"]["t"],
        "lang": article_metadata["m"]["l"],
        "journal": article_metadata["m"]["pub"],
    }

    for page_no in page_file_names:
        page = pages[page_file_names[page_no]]
        regions = [
            region
            for region in page["r"]
            if region["pOf"] == article_id
        ]
        tokens = [
            token
            for region in regions
            for para in region["p"]
            for line in para["l"]
            for token in line["t"]
        ]

        # TODO: capture and store somewhere the line breaks
        # see https://github.com/impresso/impresso-pycommons/issues/5

        if fulltext == "":
            fulltext, regions = rebuild_text(tokens)
        else:
            fulltext, regions = rebuild_text(tokens, fulltext)

        page_doc = {
            "id": page_file_names[page_no],
            "n": page_no,
            "regions": regions
        }
        article["pages"].append(page_doc)
    logger.info(f'Done rebuilding article {article_id}')
    article["text"] = fulltext
    return article


def main():
    arguments = docopt(__doc__)
    clear_output = arguments["--clear"]
    bucket_name = arguments["--input-bucket"]
    outp_dir = arguments["--output-dir"]
    output_format = arguments["--format"]
    log_file = arguments["--log-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    # Initialise the logger
    global logger
    logger.setLevel(log_level)

    if(log_file is not None):
        handler = logging.FileHandler(filename=log_file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Logger successfully initialised")

    # clean output directory if existing
    if outp_dir is not None and os.path.exists(outp_dir):
        if clear_output is not None and clear_output:
            shutil.rmtree(outp_dir)
            os.mkdir(outp_dir)

    bucket = get_bucket(bucket_name)

    def _odict_to_ntuple(odict):
        return IssueDir(**odict)

    if arguments["rebuild_articles"]:
        issues = impresso_iter_bucket(
            bucket.name,
            prefix="GDL/1950/01/",
            item_type="issue"
        )
        bag = db.from_sequence(issues, 20)
        bag = bag.map(lambda x: IssueDir(**x))
        bag = bag.map(read_issue, bucket)
        bag = bag.starmap(read_issue_pages, bucket=bucket)

        # attention, workaround: IssueDir cannot be pickled by `groupby`
        bag = bag.map(lambda x: (x[0]._asdict(), x[1]))
        # bag = bag.starmap(rejoin_articles)
        # bag = bag.starmap(rebuild_articles)
        # bag = bag.groupby(lambda x: x[0]['date'].year)
        # bag = bag.starmap(serialize_by_year)
    
        with ProgressBar():
            result = bag.compute()
        assert result is not None
        pdb.set_trace()
        # some Dask Fu for later
        # bag.groupby(lambda x: "{}-{}".format(x['date'].year, x['date'].month)).starmap(lambda k, v: (k, len(v))).compute()
        # rebuild_articles(issues, bucket.name, output_format, outp_dir)

    elif arguments["rebuild_pages"]:
        print("\nFunction not yet implemented (sorry!).\n")


if __name__ == '__main__':
    main()
