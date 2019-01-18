"""Utility functions to export data in Apache UIMA XMI format."""

import os
from pycas.cas.core.CAS import CAS
from pycas.cas.writer.XmiWriter import XmiWriter
from pycas.type.cas.TypeSystemFactory import TypeSystemFactory


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

    # create sentence-level annotations
    start_offset = 0
    for break_offset in ci.lines:
        start = start_offset
        end = break_offset
        start_offset = break_offset
        sntc = cas.createAnnotation(sentType, {'begin': start, 'end': end})
        cas.addToIndex(sntc)

    writer = XmiWriter()
    outfile_path = os.path.join(output_dir, f'{ci.id}.xmi')

    writer.write(cas, outfile_path)
