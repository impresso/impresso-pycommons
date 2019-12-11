"""Tests for the module `impresso_commons.utils.uima`."""

import os
import json
import pytest

import pkg_resources
from dask import bag as db

from impresso_commons.classes import ContentItem, ContentItemCase
from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT as impresso_s3
from impresso_commons.utils.uima import rebuilt2xmi, get_iiif_links


# TODO: add some test data from chroniclingamerica to test percentage coordinates
test_data = [
    ("s3://original-canonical-staging", "s3://canonical-rebuilt-release", "IMP", 1998, False),
    ("s3://original-canonical-staging", "s3://canonical-rebuilt-release", "indeplux", 1898, False),
]


@pytest.mark.parametrize("canonical_bucket, rebuilt_bucket, newspaper, year, pct_coordinates", test_data)
def test_rebuilt2xmi(canonical_bucket, rebuilt_bucket, newspaper, year, pct_coordinates):
    """Tests that the UIMA/XMI of rebuilt data works as expected."""

    output_dir = pkg_resources.resource_filename('impresso_commons', 'data/xmi')
    typesystem = pkg_resources.resource_filename('impresso_commons', 'data/xmi/typesystem.xml')

    rebuilt_path = os.path.join(rebuilt_bucket, newspaper, f'{newspaper}-{year}.jsonl.bz2')
    b = db.read_text(rebuilt_path, storage_options=impresso_s3)
    texts = b.compute()

    # we take only an arbitrary document as a test
    text = json.loads(texts[10])
    doc = ContentItem.from_json(data=text, case=ContentItemCase.FULL)

    iiif_mappings = get_iiif_links([doc], canonical_bucket)
    rebuilt2xmi(doc, output_dir, typesystem, iiif_mappings, pct_coordinates)

    # check that the output xmi file exists
    expected_filename = os.path.join(output_dir, f"{doc.id}.xmi")
    assert os.path.exists(expected_filename)
