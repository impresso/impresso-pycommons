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

from impresso_commons.path.path_fs import IssueDir, detect_issues
from impresso_commons.path.path_s3 import (impresso_iter_bucket,
                                           s3_detect_issues)
from impresso_commons.text.helpers import (read_issue, read_issue_pages,
                                           rejoin_articles)
from impresso_commons.utils import Timer
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
            region["coords"] = token["c"]
            region["start"] = len(string)
            region["surface"] = None

            # if token is the last in a line
            if n == len(line) - 1:
                linebreaks.append(region["start"] + len(token["tx"]))

            if "hy" in token:
                region["length"] = len(token["tx"][:-1])
                region["surface"] = token["tx"][:-1]

            elif "nf" in token:
                region["length"] = len(token["nf"])

                if "gn" in token and token["gn"]:
                    tmp = "{}".format(token["nf"])
                    region["surface"] = token["tx"]
                    string += tmp
                else:
                    tmp = "{} ".format(token["nf"])
                    region["surface"] = token["tx"]
                    string += tmp
            else:
                region["length"] = len(token["tx"])

                if "gn" in token and token["gn"]:
                    tmp = "{}".format(token["tx"])
                    region["surface"] = tmp
                    string += tmp
                else:
                    tmp = "{} ".format(token["tx"])
                    region["surface"] = tmp
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

    fulltext = ""
    linebreaks = []
    article = {
        "id": article_id,
        # "series": None,
        "pages": article_metadata["m"]["pp"],
        "datetime": d.isoformat() + 'Z',
        "lg": article_metadata["m"]["l"],
        "ppreb": [],
        "lb": []
        # "journal": article_metadata["m"]["pub"],
    }

    if 't' in article_metadata:
        article["title"]: article_metadata["m"]["t"]

    if 'pub' in article_metadata:
        article["pub"]: article_metadata["m"]["pub"]

    for n, page_no in enumerate(article['pages']):

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
            "id": page_file_names[page_no],
            "n": page_no,
            "regions": regions
        }
        article["lb"] = linebreaks
        article["ppreb"].append(page_doc)
    logger.info(f'Done rebuilding article {article_id} (Took {t.stop()})')
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
        bag = bag.starmap(rejoin_articles)
        bag = bag.flatten()
        # TODO: check output_format
        bag = bag.starmap(rebuild_for_solr)

        # attention, workaround: IssueDir cannot be pickled by `groupby`
        # bag = bag.map(lambda x: (x[0]._asdict(), x[1]))
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
