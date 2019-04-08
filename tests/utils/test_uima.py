"""Tests for the module `impresso_commons.utils.uima`."""

import json

import pkg_resources
from dask import bag as db

from impresso_commons.classes import ContentItem, ContentItemCase
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT as impresso_s3
from impresso_commons.utils.uima import rebuilt2xmi


def test_rebuilt2xmi():

    output_dir = pkg_resources.resource_filename(
        'impresso_commons',
        'data/xmi'
    )
    typesystem = pkg_resources.resource_filename(
        'impresso_commons',
        'data/xmi/typesystem.xml'
    )

    b = db.read_text(
        's3://canonical-rebuilt/GDL/GDL-1910.jsonl.bz2',
        storage_options=impresso_s3
    )
    # .map(lambda x: json.loads(x))

    texts = b.compute()
    text = json.loads(texts[10])
    doc = ContentItem.from_json(data=text, case=ContentItemCase.FULL)
    rebuilt2xmi(doc, output_dir, typesystem)
