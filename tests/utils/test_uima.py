"""Tests for the module `impresso_commons.utils.uima`."""

import json

import pkg_resources
from dask import bag as db

from impresso_commons.classes import ContentItem, ContentItemCase
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT as impresso_s3
from impresso_commons.utils.uima import rebuilt2xmi


def test_rebuilt2xmi():
    """Tests that the UIMA/XMI of rebuilt data works as expected."""

    output_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/xmi'
    )
    typesystem = pkg_resources.resource_filename(
        'impresso_commons',
        'data/xmi/typesystem.xml'
    )

    # we need to read (temporarily) data from this bucket as hyphenation
    # required a slightly different format of the solr rebuilt
    b = db.read_text(
        's3://TRANSFER/IMP/IMP-1998.jsonl.bz2',
        storage_options=impresso_s3
    )

    texts = b.compute()
    text = json.loads(texts[10])
    doc = ContentItem.from_json(data=text, case=ContentItemCase.FULL)
    rebuilt2xmi(doc, output_dir, typesystem)
    # TODO: check that file exists
