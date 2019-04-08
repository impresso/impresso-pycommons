"""Utility functions to export data in Apache UIMA XMI format."""

import os
from pycas.cas.core.CAS import CAS
from pycas.cas.writer.XmiWriter import XmiWriter
from pycas.type.cas.TypeSystemFactory import TypeSystemFactory
from impresso_commons.images.olive_boxes import get_iiif_url


# IMPRESSO_IIIF_ENDPOINT = 'https://dhlabsrv17.epfl.ch/iiif_impresso/'
IMPRESSO_IIIF_ENDPOINT = 'http://pub.cl.uzh.ch/service/iiif_impresso'


def compute_image_link(ci, start_offset, end_offset, padding=20):

    tokens = ci.get_coordinates(start_offset, end_offset)
    page_id = tokens[0]['page_id']
    first_token = tokens[0]
    last_token = tokens[-1]

    # compute box coordinates of line
    x1, y1, w1, h1 = first_token['coords']
    x2, y2, w2, h2 = last_token['coords']
    x3, y3, w3, h3 = x1, y1 - padding, w2 + (x2 - x1), h1 + padding
    box = " ".join([str(coord) for coord in [x3, y3, w3, h3]])
    import ipdb; ipdb.set_trace()
    return get_iiif_url(page_id, box, IMPRESSO_IIIF_ENDPOINT)


def rebuilt2xmi(ci, output_dir, typesystem_path):
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
    tsf = TypeSystemFactory()
    tsf = tsf.readTypeSystem(typesystem_path)
    cas = CAS(tsf)
    cas.documentText = ci.fulltext
    cas.sofaMimeType = 'text'
    sentType = 'de.tudarmstadt.ukp.dkpro.core.api.segmentation.type.Sentence'
    imgLinkType = 'webanno.custom.ImpressoImages'

    # create sentence-level annotations
    start_offset = 0
    for break_offset in ci.lines:
        start = start_offset
        end = break_offset
        start_offset = break_offset
        sntc = cas.createAnnotation(sentType, {'begin': start, 'end': end})
        iiif_link = compute_image_link(ci, start, end)
        imglink = cas.createAnnotation(
            imgLinkType,
            {'begin': start, 'end': end, 'link': iiif_link}
        )
        cas.addToIndex(sntc)
        cas.addToIndex(imglink)

    writer = XmiWriter()
    outfile_path = os.path.join(output_dir, f'{ci.id}.xmi')

    writer.write(cas, outfile_path)
