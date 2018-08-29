"""Functions and CLI to rebuild text from impresso's canonical format.

Usage:
    rebuilder.py rebuild_articles --input-bucket=<b> --log-file=<f> --output-dir=<od> --filter-config=<fc> [--output-bucket=<ob> --verbose --clear --format=<f>]
    rebuilder.py rebuild_pages --input-bucket=<b> --log-file=<f> --output-dir=<od> [--output-bucket=<ob> --verbose --clear --format=<f>]
"""  # noqa: E501

import codecs
import datetime
import json
import logging
import os
import shutil

# import dask
import dask.bag as db
import jsonlines
from dask.diagnostics import ProgressBar
from docopt import docopt
from smart_open import smart_open

from impresso_commons.path import parse_canonical_filename
from impresso_commons.path.path_fs import IssueDir
from impresso_commons.path.path_s3 import impresso_iter_bucket
from impresso_commons.text.helpers import (pages_to_article, read_issue,
                                           read_issue_pages, rejoin_articles)
from impresso_commons.utils import Timer
from impresso_commons.utils.s3 import get_bucket, get_s3_resource

logger = logging.getLogger('impresso_commons')


TYPE_MAPPINGS = {
    "article": "ar",
    "advertisement": "ad",
    "ad": "ad"
}


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


def rebuild_text(lines, string=None):
    """The text rebuilding function."""

    regions = []
    linebreaks = []

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)
    for line in lines:
        for n, token in enumerate(line):

            region = {}
            region["c"] = token["c"]
            region["s"] = len(string)

            # if token is the last in a line
            if n == len(line) - 1:
                linebreaks.append(region["s"] + len(token["tx"]))

            if "hy" in token:
                region["l"] = len(token["tx"][:-1])

            elif "nf" in token:
                region["l"] = len(token["nf"])

                if "gn" in token and token["gn"]:
                    tmp = "{}".format(token["nf"])
                    string += tmp
                else:
                    tmp = "{} ".format(token["nf"])
                    string += tmp
            else:
                region["l"] = len(token["tx"])

                if "gn" in token and token["gn"]:
                    tmp = "{}".format(token["tx"])
                    string += tmp
                else:
                    tmp = "{} ".format(token["tx"])
                    string += tmp

            regions.append(region)

    return (string, regions, linebreaks)


def rebuild_for_solr(article_metadata):
    """Rebuilds the text of an article given its metadata as input.

    :param article_metadata: the article's metadata
    :type article_metadata: dict
    :return: a dictionary with the following keys: TBD
    :rtype: dict
    """
    t = Timer()
    article_id = article_metadata["m"]["id"]
    logger.info(f'Started rebuilding article {article_id}')
    issue_id = "-".join(article_id.split('-')[:-1])
    page_file_names = {
        p: "{}-p{}.json".format(issue_id, str(p).zfill(4))
        for p in article_metadata["m"]["pp"]
    }
    year, month, day = article_id.split('-')[1:4]
    d = datetime.datetime(int(year), int(month), int(day), 5, 0, 0)
    mapped_type = TYPE_MAPPINGS[article_metadata["m"]["tp"]]

    fulltext = ""
    linebreaks = []
    article = {
        "id": article_id,
        # "series": None,
        "pp": article_metadata["m"]["pp"],
        "d": d.isoformat() + 'Z',
        "lg": article_metadata["m"]["l"],
        "tp": mapped_type,
        "ppreb": [],
        "lb": []
    }

    if 't' in article_metadata["m"]:
        article["t"] = article_metadata["m"]["t"]

    for n, page_no in enumerate(article['pp']):

        page = article_metadata['pprr'][n]
        tokens = [
            [token for token in line["t"]]
            for region in page
            for para in region["p"]
            for line in para["l"]
        ]

        if fulltext == "":
            fulltext, regions, _linebreaks = rebuild_text(tokens)
        else:
            fulltext, regions, _linebreaks = rebuild_text(tokens, fulltext)

        linebreaks += _linebreaks

        page_doc = {
            "id": page_file_names[page_no].replace('.json', ''),
            "n": page_no,
            "t": regions
        }
        article["lb"] = linebreaks
        article["ppreb"].append(page_doc)
    logger.info(f'Done rebuilding article {article_id} (Took {t.stop()})')
    article["ft"] = fulltext
    return article


def serialize(sort_key, articles, output_dir=None):
    """Serialize a bunch of articles into a compressed JSONLines archive.

    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :type sort_key: string
    :param articles: a list of JSON documents, encoded as python dictionaries
    :type: list of dict

    NB: sort_key is the concatenation of newspaper ID and year (e.g. GDL-1900).
    """
    logger.info(f"Serializing {sort_key} (n = {len(articles)})")
    newspaper, year = sort_key.split('-')
    filename = f'{newspaper}-{year}.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    with smart_open(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)
        writer.write_all(articles)
        writer.close()

    # return also number of articles?
    return sort_key, filepath


def upload(sort_key, filepath, bucket_name=None):
    # create connection with bucket
    # copy contents to s3 key
    newspaper, year = sort_key.split('-')
    key_name = "{}/{}".format(
        newspaper,
        os.path.basename(filepath)
    )
    s3 = get_s3_resource()
    try:
        bucket = s3.Bucket(bucket_name)
        bucket.upload_file(filepath, key_name)
        logger.info(f'Uploaded {filepath} to {key_name}')
        return True, filepath
    except Exception as e:
        logger.error(e)
        logger.error(f'The upoload of {filepath} failed with error {e}')
        return False, filepath


def cleanup(success, filepath):
    if success:
        os.remove(filepath)
        logger.info(f'Removed temporary file {filepath}')
    else:
        logger.info(f'Not removing {filepath} as upload has failed')


def rebuild_issues(
        issues,
        input_bucket,
        output_dir,
        output_bucket,
        clear_output
):
    """TODO"""

    def has_problem(article):
        if article['has_problem']:
            logger.warning(f"Article {article['m']['id']} won't be rebuilt.")
        return not article['has_problem']

    print(f'There are {len(issues)} issues to rebuild')
    bag = db.from_sequence(issues, 40) \
        .map(lambda x: IssueDir(**x)) \
        .map(read_issue, input_bucket) \
        .starmap(read_issue_pages, bucket=input_bucket) \
        .starmap(rejoin_articles) \
        .flatten() \
        .starmap(pages_to_article)\
        .filter(has_problem) \
        .map(rebuild_for_solr) \
        .groupby(
            lambda x: "{}-{}".format(
                parse_canonical_filename(x["id"])[0],  # e.g. GDL
                parse_canonical_filename(x["id"])[1][0][:3]  # e.g. 195
            )
        )\
        .starmap(serialize, output_dir=output_dir)

    if output_bucket is not None:
        bag = bag.starmap(upload, bucket_name=output_bucket)

        if clear_output:
            bag = bag.starmap(cleanup)

    with ProgressBar():
        result = bag.compute()

    return result


def main():
    arguments = docopt(__doc__)
    clear_output = arguments["--clear"]
    bucket_name = arguments["--input-bucket"]
    output_bucket_name = arguments["--output-bucket"]
    outp_dir = arguments["--output-dir"]
    filter_config_file = arguments["--filter-config"]
    # output_format = arguments["--format"]
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

    # there was a connection error issue with s3
    with open(filter_config_file, 'r') as file:
        config = json.load(file)

    bucket = get_bucket(bucket_name)

    if arguments["rebuild_articles"]:

        print('Retrieving issues...')
        issues = impresso_iter_bucket(
            bucket_name,
            filter_config=config,
            # prefix="GDL/1948/09/03",
            item_type="issue"
        )

        result = rebuild_issues(
            issues,
            bucket,
            outp_dir,
            output_bucket_name,
            clear_output
        )

        assert result is not None
        # import pdb; pdb.set_trace()

    elif arguments["rebuild_pages"]:
        print("\nFunction not yet implemented (sorry!).\n")


if __name__ == '__main__':
    main()
