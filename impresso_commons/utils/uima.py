"""Utility functions to export data in Apache UIMA XMI format."""

import os
from pycas.cas.core.CAS import CAS
from pycas.cas.writer.XmiWriter import XmiWriter
from pycas.type.cas.TypeSystemFactory import TypeSystemFactory


def rebuilt2xmi(document, output_dir, typesystem_path):
    """
    TODO
    """
    tsf = TypeSystemFactory()
    tsf = tsf.readTypeSystem(typesystem_path)
    cas = CAS(tsf)
    cas.documentText = document.fulltext
    cas.sofaMimeType = 'text'

    # TODO: use paragraph breaks to simulate sentence breaks

    writer = XmiWriter()
    outfile_path = os.path.join(output_dir, f'{document.id}.xmi')

    writer.write(cas, outfile_path)
