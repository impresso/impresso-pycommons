"""Utility functions to export data in Apache UIMA XMI format."""

import os
import json
from typing import Dict, Tuple, List

from dask import bag as db
from cassis import load_cas_from_xmi, load_typesystem, Cas


from impresso_commons.utils.s3 import IMPRESSO_STORAGEOPT
from impresso_commons.classes import ContentItem
from impresso_commons.images.olive_boxes import get_iiif_url

IMPRESSO_IIIF_ENDPOINT = 'https://dhlabsrv17.epfl.ch/iiif_impresso/'
# IMPRESSO_IIIF_ENDPOINT = 'http://pub.cl.uzh.ch/service/iiif_impresso'


def compute_image_links(
    ci: ContentItem,
    padding: int = 20,
    iiif_endpoint: str = IMPRESSO_IIIF_ENDPOINT,
    iiif_links: Dict[str, str] = None,
    pct: bool = False,
):
    """Short summary.

    :param type ci: Description of parameter `ci`.
    :param type padding: Description of parameter `padding`.
    :return: Description of returned object.
    :rtype: type

    """

    image_links = []
    start_offset = 0

    for line_n, break_offset in enumerate(ci.lines):
        start = start_offset
        end = break_offset
        start_offset = break_offset

        tokens = ci.get_coordinates(start, end)

        if len(tokens) == 0:
            continue

        page_id = tokens[0]['page_id']

        if 'hy1' in tokens[0] and len(tokens) > 1:
            first_token = tokens[1]
        else:
            first_token = tokens[0]

        last_token = tokens[-1]

        if line_n + 1 < len(ci.lines):
            next_offset = ci.lines[line_n + 1]
            next_line_tokens = ci.get_coordinates(start_offset, next_offset)

            if len(next_line_tokens) > 0 and 'hy1' in next_line_tokens[0]:
                last_token = next_line_tokens[0]
            else:
                last_token = tokens[-1]

        # compute box coordinates of line
        x1, y1, w1, h1 = first_token['coords']
        x2, y2, w2, h2 = last_token['coords']
        x3, y3, w3, h3 = x1, y1 - padding, w2 + (x2 - x1), h1 + padding
        box = " ".join([str(coord) for coord in [x3, y3, w3, h3]])
        if iiif_links is None:
            iiif_link = get_iiif_url(page_id, box, IMPRESSO_IIIF_ENDPOINT, pct)
        else:
            iiif_link = get_iiif_url(page_id, box, iiif_manifest_uri=iiif_links[page_id], pct=pct)
        image_links.append((iiif_link, start, end))

    return image_links


def get_iiif_links(contentitems: List[ContentItem], canonical_bucket: str):
    """Retrieves from S3 IIIF links for a set of canonical pages where the input content items are found."""

    # derive the IDs of all issues involved
    issue_ids = set(["-".join(ci.id.split('-')[:-1]) for ci in contentitems])

    # reconstruct S3 links to canonical pages
    page_files = [
        os.path.join(
            canonical_bucket,
            issue_id.split('-')[0],
            "pages",
            f"{issue_id.split('-')[0]}-{issue_id.split('-')[1]}",
            f"{issue_id}-pages.jsonl.bz2",
        )
        for issue_id in issue_ids
    ]

    iiif_links = (
        db.read_text(page_files, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .map(lambda x: (x['id'], x['iiif']))
        .compute()
    )

    return {page_id: iiif_link for page_id, iiif_link in iiif_links}


def rebuilt2xmi(ci, output_dir, typesystem_path, iiif_mappings, pct_coordinates=False) -> str:
    """
    Converts a rebuilt ContentItem into Apache UIMA/XMI format.

    The resulting file will be named after the content item's ID, adding
    the `.xmi` extension.

    :param ci: the content item to be converted
    :type ci: `impresso_commons.classes.ContentItem`
    :param output_dir: the path to the output directory
    :type output_dir: str
    :param typesystem_path: TypeSystem file containing defitions of annotation
    layers.
    :type typesystem_path: str
    """

    with open(typesystem_path, "rb") as f:
        typesystem = load_typesystem(f)

    cas = Cas(typesystem=typesystem)
    cas.sofa_string = ci.fulltext
    cas.sofa_mime = 'text/plain'

    sentType = 'de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence'
    imgLinkType = 'webanno.custom.ImpressoImages'
    Sentence = typesystem.get_type(sentType)
    ImageLink = typesystem.get_type(imgLinkType)

    # create sentence-level annotations
    start_offset = 0
    for break_offset in ci.lines:
        start = start_offset
        end = break_offset
        start_offset = break_offset
        cas.add_annotation(Sentence(begin=start, end=end))

    iiif_links = compute_image_links(ci, iiif_links=iiif_mappings, pct=pct_coordinates)

    # inject the IIIF links into
    for iiif_link, start, end in iiif_links:
        cas.add_annotation(ImageLink(begin=start, end=end, link=iiif_link))

    outfile_path = os.path.join(output_dir, f'{ci.id}.xmi')
    cas.to_xmi(outfile_path, pretty_print=True)
    return outfile_path
