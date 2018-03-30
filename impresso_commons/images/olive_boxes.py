#!/usr/bin/python3
"""
Functions to support re-computation of Olive box coordinates.
"""

import logging
import cv2 as cv
from bs4 import BeautifulSoup
import os
import zipfile
import numpy as np

from impresso_commons.images import img_utils

__author__ = "maudehrmann"


logger = logging.getLogger(__name__)


def get_iiif_url(page_id, box):
    """ Returns impresso iiif url given a page id and a box

    :param page_id: impresso page id, e.g. EXP-1930-06-10-a-p0001
    :type page_id: str
    :param box: iiif box (x, y, w, h)
    :type box: str (4 coordinate values blank separated)
    :return: iiif url of the box
    :rtype: str
    """
    base = "http://dhlabsrv17.epfl.ch/iiif_impresso"
    suffix = "full/0/default.jpg"
    return os.path.join(base, page_id, box.replace(" ", ","), suffix)


def compute_scale_factor(img_source_path, img_dest_path):
    """ Computes x scale factor bewteen 2 images

    :param img_source_path: the source image
    :type img_source_path: full path to the image
    :param img_dest_path: the destination image
    :type img_dest_path: full path to the image
    :return: scale factor
    :rtype:float

    """

    img_s = cv.imread(img_source_path, 0)
    img_d = cv.imread(img_dest_path, 0)
    x_s = img_s.shape[0]
    x_d = img_d.shape[0]
    return x_d / x_s


def compute_box(scale_factor, input_box):
    """
    Compute IIIF box coordinates of input_box relative to scale_factor.

    :param scale_factor: ratio between 2 images with different dimensions
    :type scale_factor: float
    :param input_box: string with 4 values separated by spaces
    :type input_box: str
    :return: new box coordinates
    :rtype: str
    """
    try:
        elems = input_box.split(" ")
    except ValueError:
        logger.info(f'Invalid box format: {input_box}')
        return

    x = round(int(elems[0]) * scale_factor)
    y = round(int(elems[1]) * scale_factor)
    w = round((int(elems[2]) - int(elems[0])) * scale_factor)
    h = round((int(elems[3]) - int(elems[1])) * scale_factor)
    return " ".join([str(x), str(y), str(w), str(h)])


def convert_box(input_box):
    """
    Convert a box with [x y x y] coordinates to [x y w h]

    :param input_box: box with 4 coordinates, x and y upper left and lower right
    :type input_box: str
    :return: box with 4 coordinates, x and y upper left, width and height
    :rtype: str
    """
    try:
        elems = input_box.split(" ")
    except ValueError:
        logger.info(f'Invalid box format: {input_box}')
        return

    w = (int(elems[2]) - int(elems[0]))
    h = (int(elems[3]) - int(elems[1]))
    return ",".join([elems[0], elems[1], str(w), str(h)])


def get_scale_factor(issue_dir_path, archive, page_xml, box_strategy, img_source_name):
    """
    Returns the scale factor in Olive context, given a strategy to choose the source image.

    :param issue_dir_path: the path of the issue
    :type  issue_dir_path: str
    :param archive: the zip archive
    :type  archive: zipfile.ZipFile
    :param page_xml: the xml handler of the page
    :type page_xml: bytes
    :param box_strategy: the box strategy such as found in the info.txt from jp2 folder
    :type box_strategy: str
    :param img_source_name: as found in the info.txt from jp2 folder
    :return: the hopefully correct scale factor
    :rtype: float

    Olive box coordinates were computed according to an image source which we have to identify.
    Image format coverage is different from issue to issue, and we have to devise strategies.

    Case 1: tif. The tif is present and is the file from which the jp2 was converted.
    Dest: Tif dimensions can therfore be used as jp2 dimensions, no need to read the jp2 file.
    Source: Image source dimension is present in the page.xml (normally).

    Case 2: several png. In this case the jp2 was acquired using the png with the highest dimension.
    Dest: It looks that in case of several png, Olive also took the highest for the OCR. It is therefore
    possible to rely on the resolution indicated in the page xml, which should be the same as our jp2.
    N.B.: the page width and heigth indicated in the xml do not correspond (usually) to the highest
    resolution png (there is therefore a discrepancy in Olive file between the tag 'images_resolution'
    on the one hand, and 'page_width|height'on the other). It seems we can ignore this and rely on the
    resolution only in the current case.
    Source: the highest png
    Here source and dest dimension are equals, the function returns 1.

    Case 3: one png only. To be checked if it happens.
    In this case, there is no choice and Olive OCR and JP2 acquisition should be from the same source
    => scale factor of 1.
    Here we do an additional check to see if the page_width|height are the same as the image ones.
    The only danger is if Olive used another image file and did not provide it.

    Case 4: one jpg only.
    Same as Case 3, scale factor of 1.
    Here we do an additional check to see if the page_width|height are the same as the image ones.
    (there is only one image and things should fit, not like in case 2)
    """

    page_soup = BeautifulSoup(page_xml, "lxml")
    page_root = page_soup.find("xmd-page")
    page_number = page_root.meta["page_no"]

    if box_strategy == img_utils.BoxStrategy.tif.name:
        for f in page_root.datafiles.find_all('files'):
            if f['type'] == 'PAGE_IMG' and f['present'] == "1":
                source_res = f['xresolution_dpi']
                dest_res = page_root.meta['images_resolution']
                break
        if source_res and dest_res:
            return int(source_res) / int(dest_res)
        else:
            logger.info("Impossible to get resolution in case: tif"
                        " in {issue_dir_path}, page {page_number}")
            return None

    elif box_strategy == img_utils.BoxStrategy.png_highest.name:
        if "_" not in img_source_name:
            logger.info(f"Not valid png filename {img_source_name}")
            return None

        png_res = os.path.splitext(img_source_name)[0].split("_", 1)[-1]
        olive_res = page_root.meta['images_resolution']
        if png_res == olive_res:
            return 1.0
        else:
            logger.info("Incompatible resolutions between highest png and olive indications"
                        " in {issue_dir_path}, page {page_number}")
            return None

    elif box_strategy == img_utils.BoxStrategy.png_uniq.name:
        # TODO if needed
        logger.info("Finally found a case of png_uniq, which is not ready yet")

    elif box_strategy == img_utils.BoxStrategy.jpg_uniq.name:
        # get the x dimension of the unique jpg (from which jp2 was acquired)
        # and compare with olive's one
        img_data = archive.read(img_source_name)
        img = cv.imdecode(np.frombuffer(img_data, np.uint8), 1)
        jpg_x_dim = img.shape[1]

        olive_x_dim = page_root.meta['page_width']

        if jpg_x_dim == int(olive_x_dim):
            return 1.0
        else:
            logger.info("Incompatible resolutions between uniq jpg and olive indications"
                        " in {issue_dir_path}, page {page_number}.")
            return None


def test():
    """ a testing function """
    base_dir = "/Users/maudehrmann/work/work-projects/impresso/code/impresso-image-acquisition/sample-data"

    # tifs
    box = "262 624 335 650"  # 'Centenaire' in p1 Ar00100.xml
    archive = os.path.join(base_dir, "TEST/1900/01/10/Document.zip")
    working_archive = zipfile.ZipFile(archive)
    data = working_archive.read("1/Pg001.xml")
    sf = get_scale_factor("fictious path", working_archive, data, img_utils.BoxStrategy.tif.name, None)
    newbox = compute_box(sf, box)
    iiif = get_iiif_url("GDL-1900-01-10-a-p0001", newbox)
    print("\nCASE: jpj uniq - word 'Centenaire'")
    print(f"Scale factor: {sf}")
    print(f"Newbox: {newbox}")
    print(f"IIIF: {iiif}")

    # png_highest
    box = "1047 1006 1173 1036"  # 'NEUCHATEL' in p1 Ad00103.xml
    archive = os.path.join(base_dir, "TEST/1900/01/11/Document.zip")
    working_archive = zipfile.ZipFile(archive)
    data = working_archive.read("1/Pg001.xml")
    sf = get_scale_factor("fictious path", working_archive, data, img_utils.BoxStrategy.png_highest.name, "Img/Pg001_180.png")
    print(sf)
    newbox = compute_box(sf, box)
    iiif = get_iiif_url("EXP-1889-07-01-a-p0001", newbox)
    print("\nCASE: png_highest - word 'NEUCHATEL'")
    print(f"Scale factor: {sf}")
    print(f"Newbox: {newbox}")
    print(f"IIIF: {iiif}")

    # jpg_uniq
    box = "483 502 556 517"  # Hambourg in p1 Ar00100.xml
    archive = os.path.join(base_dir, "TEST/1900/01/12/Document.zip")
    working_archive = zipfile.ZipFile(archive)
    page_data = working_archive.read("1/Pg001.xml")
    sf = get_scale_factor("fictious path", working_archive, page_data, img_utils.BoxStrategy.jpg_uniq.name, "1/Img/Pg001.jpg")
    newbox = compute_box(sf, box)
    iiif = get_iiif_url("LCE-1868-08-02-a-p0001", newbox)
    print("\nCASE: jpj uniq - word 'hambourg'")
    print(f"Scale factor: {sf}")
    print(f"Newbox: {newbox}")
    print(f"IIIF: {iiif}")


if __name__ == '__main__':
    # test()
    box = "1096 454 1200 470"
    print(convert_box(box))