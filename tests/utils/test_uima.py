"""Tests for the module `impresso_commons.utils.uima`."""

from dask import bag as db
from impresso_commons.text import RebuiltDocument
from impresso_commons.utils.uima import rebuilt2xmi
import pkg_resources
import os
import json


def test_rebuilt2xmi():
    my_access = os.environ['SE_ACCESS_KEY']
    my_secret = os.environ['SE_SECRET_KEY']
    host = 'https://os.zhdk.cloud.switch.ch'

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
        # replace with `impresso_s3`
        storage_options={
            'client_kwargs': {'endpoint_url': host},
            'key': my_access, 'secret': my_secret
         }
    )
    # .map(lambda x: json.loads(x))

    texts = b.compute()
    text = json.loads(texts[10])
    doc = RebuiltDocument.from_json(data=text)
    print(doc)
    rebuilt2xmi(doc, output_dir, typesystem)
