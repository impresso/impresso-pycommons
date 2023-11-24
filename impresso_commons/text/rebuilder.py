"""Functions and CLI to rebuild text from impresso's canonical format.

Usage:
    rebuilder.py rebuild_articles --input-bucket=<b> --log-file=<f> --output-dir=<od> --filter-config=<fc> [--format=<fo> --scheduler=<sch> --output-bucket=<ob> --verbose --clear --languages=<lgs> --nworkers=<nw>]

Options:

--input-bucket=<b>  S3 bucket where canonical JSON data will be read from
--output-bucket=<ob>  Rebuilt data will be uploaded to the specified s3 bucket (otherwise no upload)
--log-file=<f>  Path to log file
--scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
--filter-config=<fc>  A JSON configuration file specifying which newspaper issues will be rebuilt
--verbose  Set logging level to DEBUG (by default is INFO)
--clear  Remove output directory before and after rebuilding
--format=<fo>  stuff
--nworkers=<nw>  number of workers for (local) dask client
"""  # noqa: E501

import traceback
import datetime
import json
import pathlib
import logging
import os
import shutil
import signal
from sys import exit

import dask.bag as db
import jsonlines
from dask.distributed import Client, progress
from docopt import docopt
from smart_open import smart_open

from impresso_commons.path import parse_canonical_filename
from impresso_commons.path.path_fs import IssueDir
from impresso_commons.path.path_s3 import read_s3_issues
from impresso_commons.text.helpers import (read_issue_pages, rejoin_articles)
from impresso_commons.utils import Timer, timestamp
from impresso_commons.utils.s3 import get_s3_resource

logger = logging.getLogger(__name__)


TYPE_MAPPINGS = {
    "article": "ar",
    "ar": "ar",
    "advertisement": "ad",
    "ad": "ad",
    "pg": None,
    "image": "img",
    "table": "tb",
    "death_notice": "ob",
    "weather": "w"
}


def rebuild_text(page, string=None):
    """The text rebuilding function.

    :param page: a newspaper page conforming to the impresso JSON schema
        for pages.
    :type page: dict
    :param string: the rebuilt text of the previous page. If `string` is not
        `None`, then the rebuilt text is appended to it.
    :type string: str
    :return: a tuple with: [0] fulltext, [1] offsets (dict of lists) and
        [2] coordinates of token regions (dict of lists).
    """

    coordinates = {
        "regions": [],
        "tokens": []
    }

    offsets = {
        "line": [],
        "para": [],
        "region": []
    }

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)
    for region_n, region in enumerate(page):

        if len(string) > 0:
            offsets['region'].append(len(string))

        coordinates['regions'].append(region['c'])

        for i, para in enumerate(region["p"]):

            if len(string) > 0:
                offsets['para'].append(len(string))

            for line in para["l"]:

                for n, token in enumerate(line['t']):
                    region = {}
                    region["c"] = token["c"]
                    region["s"] = len(string)

                    if "hy" in token:
                        region["l"] = len(token["tx"][:-1])-1
                        region['hy1'] = True

                    elif "nf" in token:
                        region["l"] = len(token["nf"])
                        region['hy2'] = True

                        if "gn" in token and token["gn"]:
                            tmp = "{}".format(token["nf"])
                            string += tmp
                        else:
                            tmp = "{} ".format(token["nf"])
                            string += tmp
                    else:
                        if token['tx']:
                            region["l"] = len(token["tx"])
                        else:
                            region["l"] = 0

                        if "gn" in token and token["gn"]:
                            tmp = "{}".format(token["tx"])
                            string += tmp
                        else:
                            tmp = "{} ".format(token["tx"])
                            string += tmp

                    # if token is the last in a line
                    if n == len(line['t']) - 1:
                        if 'hy' in token:
                            offsets['line'].append(region["s"])
                        else:
                            token_length = len(token["tx"]) if token['tx']\
                             else 0
                            offsets['line'].append(
                                region["s"] + token_length
                            )

                    coordinates['tokens'].append(region)

    return (string, coordinates, offsets)


def rebuild_text_passim(page, string=None):
    """The text rebuilding function.

    :param page: a newspaper page conforming to the impresso JSON schema
        for pages.
    :type page: dict
    :param string: the rebuilt text of the previous page. If `string` is not
        `None`, then the rebuilt text is appended to it.
    :type string: str
    :return: a tuple with: [0] fulltext, [1] offsets (dict of lists) and
        [2] coordinates of token regions (dict of lists).
    """

    regions = []

    if string is None:
        string = ""

    # in order to be able to keep line break information
    # we iterate over a list of lists (lines of tokens)

    for region_n, region in enumerate(page):

        for i, para in enumerate(region["p"]):

            for line in para["l"]:

                for n, token in enumerate(line['t']):

                    region_string = ""

                    # each page region is a token
                    output_region = {
                        "start": None,
                        "length": None,
                        'coords': {
                            "x": token['c'][0],
                            "y": token['c'][1],
                            "w": token['c'][2],
                            "h": token['c'][3]
                        }
                    }

                    if len(string) == 0:
                        output_region['start'] = 0
                    else:
                        output_region['start'] = len(string)

                    # if token is the last in a line
                    if n == len(line['t']) - 1:
                        tmp = "{}\n".format(token["tx"])
                        region_string += tmp
                    elif "gn" in token and token["gn"]:
                        tmp = "{}".format(token["tx"])
                        region_string += tmp
                    else:
                        tmp = "{} ".format(token["tx"])
                        region_string += tmp

                    string += region_string
                    output_region['length'] = len(region_string)
                    regions.append(output_region)

    return (string, regions)


def rebuild_for_solr(article_metadata):
    """Rebuilds the text of an article given its metadata as input.

    .. note::

        This rebuild function is thought especially for ingesting the newspaper
        data into our Solr index.

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
    d = datetime.date(int(year), int(month), int(day))
    raw_type = article_metadata["m"]["tp"]

    if raw_type in TYPE_MAPPINGS:
        mapped_type = TYPE_MAPPINGS[raw_type]
    else:
        mapped_type = raw_type

    fulltext = ""
    linebreaks = []
    parabreaks = []
    regionbreaks = []

    article = {
        "id": article_id,
        "pp": article_metadata["m"]["pp"],
        "d": d.isoformat(),
        "olr": False if mapped_type is None else True,
        "ts": timestamp(),
        "lg": article_metadata["m"]["l"] if "l" in article_metadata["m"]
        else None,
        "tp": mapped_type,
        "s3v": article_metadata["m"]["s3v"] if "s3v" in article_metadata["m"]
        else None,
        "ppreb": [],
        "lb": [],
        "cc": article_metadata["m"]["cc"]
    }

    if mapped_type == "img":
        suffix = "full/0/default.jpg"
        if (
            "iiif_link" in article_metadata["m"] and
            article_metadata["m"]['iiif_link'] is not None
        ):
            iiif_link = article_metadata["m"]["iiif_link"]
            article['iiif_link'] = os.path.join(
                os.path.dirname(iiif_link),
                ",".join([str(c) for c in article_metadata["c"]]),
                suffix
            )
        else:
            article['iiif_link'] = None

    if 't' in article_metadata["m"]:
        article["t"] = article_metadata["m"]["t"]

    if mapped_type != "img":
        for n, page_no in enumerate(article['pp']):

            page = article_metadata['pprr'][n]

            if fulltext == "":
                fulltext, coords, offsets = rebuild_text(page)
            else:
                fulltext, coords, offsets = rebuild_text(page, fulltext)

            linebreaks += offsets['line']
            parabreaks += offsets['para']
            regionbreaks += offsets['region']

            page_doc = {
                "id": page_file_names[page_no].replace('.json', ''),
                "n": page_no,
                "t": coords['tokens'],
                "r": coords['regions']
            }
            article["ppreb"].append(page_doc)
        article["lb"] = linebreaks
        article["pb"] = parabreaks
        article["rb"] = regionbreaks
        logger.info(f'Done rebuilding article {article_id} (Took {t.stop()})')
        article["ft"] = fulltext
    return article


def rebuild_for_passim(article_metadata):
    np, date, edition, ci_type, ci_number, ext = parse_canonical_filename(
        article_metadata['m']['id']
    )

    article_id = article_metadata["m"]["id"]
    logger.info(f'Started rebuilding article {article_id}')
    issue_id = "-".join(article_id.split('-')[:-1])

    page_file_names = {
        p: "{}-p{}.json".format(issue_id, str(p).zfill(4))
        for p in article_metadata["m"]["pp"]
    }

    passim_document = {
        "series": np,
        "date": f'{date[0]}-{date[1]}-{date[2]}',
        "id": article_metadata['m']['id'],
        "cc": article_metadata["m"]["cc"],
        "lg": article_metadata["m"]['l'] if "l" in article_metadata["m"]
        else None,
        "pages": []
    }

    if 't' in article_metadata["m"]:
        passim_document['title'] = article_metadata["m"]["t"]

    fulltext = ""
    for n, page_no in enumerate(article_metadata['m']['pp']):

        page = article_metadata['pprr'][n]

        if fulltext == "":
            fulltext, regions = rebuild_text_passim(page)
        else:
            fulltext, regions = rebuild_text_passim(page, fulltext)

        page_doc = {
            "id": page_file_names[page_no].replace('.json', ''),
            "seq": page_no,
            "regions": regions
        }
        passim_document["pages"].append(page_doc)

    passim_document["text"] = fulltext

    return passim_document


def compress(key, json_files, output_dir):
    """Merges a set of JSON line files into a single compressed archive.

    :param key: signature of the newspaper issue (e.g. GDL-1900)
    :type key: str
    :param json_files: input JSON line files
    :type json_files: list
    :param output_dir: directory where to write the output file
    :type outp_dir: str
    :return: a tuple with: sorting key [0] and path to serialized file [1].
    :rytpe: tuple

    .. note::

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
    """

    newspaper, year = key.split('-')
    filename = f'{newspaper}-{year}.jsonl.bz2'
    filepath = os.path.join(output_dir, filename)
    logger.info(f'Compressing {len(json_files)} JSON files into {filepath}')
    print(f'Compressing {len(json_files)} JSON files into {filepath}')

    with smart_open(filepath, 'wb') as fout:
        writer = jsonlines.Writer(fout)

        for json_file in json_files:
            with open(json_file, 'r') as inpf:
                reader = jsonlines.Reader(inpf)
                articles = list(reader)
                writer.write_all(articles)
            logger.info(
                f'Written {len(articles)} docs from {json_file} to {filepath}'
            )

        writer.close()

    for json_file in json_files:
        os.remove(json_file)

    temp_dir = os.path.dirname(json_files[0])
    os.rmdir(temp_dir)
    logger.info(f'Removed temporary directory and files in {temp_dir}')

    return (key, filepath)


def upload(sort_key, filepath, bucket_name=None):
    """Uploads a file to a given S3 bucket.

    :param sort_key: the key used to group articles (e.g. "GDL-1900")
    :type sort_key: str
    :param filepath: path of the file to upload to S3
    :type filepath: str
    :param bucket_name: name of S3 bucket where to upload the file
    :type bucket_name: str
    :return: a tuple with [0] whether the upload was successful (boolean) and
        [1] the path of the uploaded file (string)

    .. note::

        `sort_key` is expected to be the concatenation of newspaper ID and year
        (e.g. GDL-1900).
    """
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
        logger.error(f'The upload of {filepath} failed with error {e}')
        return False, filepath


def cleanup(upload_success, filepath):
    """Removes a file if it has been successfully uploaded to S3.

    :param upload_success: whether the upload was successful
    :type upload_success: bool
    :param filepath: path to the uploaded file
    :type filepath: str
    """
    if upload_success:
        os.remove(filepath)
        logger.info(f'Removed temporary file {filepath}')
    else:
        logger.info(f'Not removing {filepath} as upload has failed')


def _article_has_problem(article):
    """Helper function to keep articles with problems.

    :param article: input article
    :type article: dict
    :return: `True` or `False`
    :rtype: boolean
    """
    return article['has_problem']


def _article_without_problem(article):
    """Helper function to keep articles without problems.

    :param article: input article
    :type article: dict
    :return: `True` or `False`
    :rtype: boolean
    """
    if article['has_problem']:
        logger.warning(f"Article {article['m']['id']} won't be rebuilt.")
    return not article['has_problem']


def rebuild_issues(
        issues,
        input_bucket,
        output_dir,
        dask_client,
        format='solr',
        filter_language=None
):
    """Rebuild a set of newspaper issues into a given format.

    :param issues: issues to rebuild
    :type issues: list of `IssueDir` objects
    :param input_bucket: name of input s3 bucket
    :type input_bucket: str
    :param outp_dir: local directory where to store the rebuilt files
    :type outp_dir: str
    :return: a list of tuples (see return type of `upload`)
    :rtype: list of tuples
    """

    def mkdir(path):
        if not os.path.exists(path):
            pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        else:
            for f in os.listdir(path):
                os.remove(os.path.join(path, f))

    # determine which rebuild function to apply
    if format == 'solr':
        rebuild_function = rebuild_for_solr
    elif format == 'passim':
        rebuild_function = rebuild_for_passim
    else:
        raise

    # create a temporary output directory named after newspaper and year
    # e.g. IMP-1994
    issue, issue_json = issues[0]
    key = f'{issue.journal}-{issue.date.year}'
    issue_dir = os.path.join(output_dir, key)
    mkdir(issue_dir)

    print("Fleshing out articles by issue...") # warning about large graph comes here
    issues_bag = db.from_sequence(issues, partition_size=3)

    faulty_issues = issues_bag.filter(
        lambda i: len(i[1]['pp']) == 0
    ).map(lambda i: i[1]).pluck('id').compute()
    logger.debug(f'Issues with no pages (will be skipped): {faulty_issues}')
    print(f'Issues with no pages (will be skipped): {faulty_issues}')
    del faulty_issues
    logger.debug(f"Number of partitions: {issues_bag.npartitions}")
    print(f"Number of partitions: {issues_bag.npartitions}")

    articles_bag = issues_bag.filter(lambda i: len(i[1]['pp']) > 0)\
        .starmap(read_issue_pages, bucket=input_bucket)\
        .starmap(rejoin_articles) \
        .flatten().persist()

    faulty_articles_n = articles_bag\
        .filter(_article_has_problem)\
        .pluck('m')\
        .pluck('id')\
        .compute()
    logger.debug(f'Skipped articles: {faulty_articles_n}')
    print(f'Skipped articles: {faulty_articles_n}')
    del faulty_articles_n

    articles_bag = articles_bag.filter(_article_without_problem)\
        .map(rebuild_function)\
        .persist()

    def has_language(ci):
        if 'lg' not in ci:
            return False
        else:
            return ci['lg'] in filter_language

    if filter_language:
        filtered_articles = articles_bag.filter(has_language).persist()
        print(filtered_articles.count().compute())
        result = filtered_articles.map(json.dumps)\
            .to_textfiles('{}/*.json'.format(issue_dir))
    else:
        result = articles_bag.map(json.dumps)\
            .to_textfiles('{}/*.json'.format(issue_dir))

    dask_client.cancel(issues_bag)  
    logger.info("done.")
    print("done.")

    return (key, result)


def init_logging(level, file):
    """Initialises the root logger.

    :param level: desired level of logging (default: logging.INFO)
    :type level: int
    :param file:
    :type file: str
    :return: the initialised logger
    :rtype: `logging.RootLogger`

    .. note::

        It's basically a duplicate of `impresso_commons.utils.init_logger` but
        I could not get it to work properly, so keeping this duplicate.
    """
    # Initialise the logger
    root_logger = logging.getLogger('')
    root_logger.setLevel(level)

    if(file is not None):
        handler = logging.FileHandler(filename=file, mode='w')
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.info("Logger successfully initialised")

    return root_logger


def main():

    def signal_handler(*args):
        # Handle any cleanup here
        print(
            'SIGINT or CTRL-C detected. Exiting gracefully'
            ' and shutting down the dask local cluster'
        )
        client.shutdown()
        exit(0)

    arguments = docopt(__doc__)
    clear_output = arguments["--clear"]
    bucket_name = f's3://{arguments["--input-bucket"]}'
    output_bucket_name = arguments["--output-bucket"]
    outp_dir = arguments["--output-dir"]
    filter_config_file = arguments["--filter-config"]
    output_format = arguments["--format"]
    scheduler = arguments["--scheduler"]
    log_file = arguments["--log-file"]
    nworkers = arguments["--nworkers"] if arguments["--nworkers"] else 8
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    languages = arguments["--languages"]

    signal.signal(signal.SIGINT, signal_handler)

    if languages:
        languages = languages.split(',')

    init_logging(log_level, log_file)

    # clean output directory if existing
    if outp_dir is not None and os.path.exists(outp_dir):
        if clear_output is not None and clear_output:
            shutil.rmtree(outp_dir)
            os.mkdir(outp_dir)

    with open(filter_config_file, 'r') as file:
        config = json.load(file)

    # start the dask local cluster
    if scheduler is None:
        client = Client(n_workers=nworkers, threads_per_worker=1)
    else:
        cluster = None
        client = Client(scheduler)
    logger.info(f"Dask local cluster: {client}")
    print(f"Dask local cluster: {client}")

    if arguments["rebuild_articles"]:

        try:
            for n, batch in enumerate(config):
                rebuilt_issues = []
                logger.info(f'Processing batch {n + 1}/{len(config)} [{batch}]')
                print(f'Processing batch {n + 1}/{len(config)} [{batch}]')
                newspaper = list(batch.keys())[0]
                start_year, end_year = batch[newspaper]

                for year in range(start_year, end_year):
                    logger.info(f'Processing year {year}')
                    logger.info('Retrieving issues...')
                    print(f'Processing year {year}')
                    print('Retrieving issues...')
                    try:
                        input_issues = read_s3_issues(
                            newspaper,
                            year,
                            bucket_name
                        )
                    except FileNotFoundError:
                        logger.info(f'{newspaper}-{year} not found in {bucket_name}')
                        print(f'{newspaper}-{year} not found in {bucket_name}')
                        continue

                    issue_key, json_files = rebuild_issues(
                        issues=input_issues,
                        input_bucket=bucket_name,
                        output_dir=outp_dir,
                        dask_client=client,
                        format=output_format,
                        filter_language=languages
                    )
                    rebuilt_issues.append((issue_key, json_files))
                    del input_issues
                logger.info((
                    f"Uploading {len(rebuilt_issues)} rebuilt bz2files "
                    f"to {output_bucket_name}"
                ))
                print((
                    f"Uploading {len(rebuilt_issues)} rebuilt bz2files "
                    f"to {output_bucket_name}"
                ))
                b = db.from_sequence(rebuilt_issues) \
                    .starmap(compress, output_dir=outp_dir) \
                    .starmap(upload, bucket_name=output_bucket_name) \
                    .starmap(cleanup)
                future = b.persist()
                progress(future)
                # clear memory of objects once computations are done
                client.restart()
                print(f"Restarted client after finishing processing batch {n + 1}")
                logger.info(f"Restarted client after finishing processing batch {n + 1}")

        except Exception as e:
            traceback.print_tb(e.__traceback__)
            print(e)
            client.shutdown()
        finally:
            client.shutdown()

        logger.info("---------- Done ----------")
        print("---------- Done ----------")

    elif arguments["rebuild_pages"]:
        print("\nFunction not yet implemented (sorry!).\n")


if __name__ == '__main__':
    main()
