#!/usr/bin/python3
# coding: utf-8 


from enum import Enum
from collections import defaultdict
import os
import cv2 as cv
import numpy as np

__author__ = "maudehrmann"


class BoxStrategy(Enum):
    tif = "tif"
    png_highest = "png_highest"
    png_uniq = "png_uniq"
    jpg_uniq = "jpg_uniq"
    jpg_highest = "jpg_highest"


def get_img_from_archive(archive, path_checker, ext_checker, name_checker=None):
    if not name_checker:
        name_checker = ""
    items = sorted(
        [
            item
            for item in archive.namelist()
            if not item.startswith(".")
               and ext_checker in item
               and path_checker in item
               and name_checker in item
        ]
    )
    return items


def get_page_folders(archive):
    dirs = {os.path.dirname(item) for item in archive.namelist()}
    topdirs = {d.split("/", 1)[0] for d in dirs}
    page_folders = [os.path.join(os.path.dirname(archive.filename), page) for page in topdirs if page.isdigit()]
    return sorted(page_folders)


def get_tif(tifs, page_digit):
    # tif files have the form of 'Res/PageImg/Page0001.tif'
    for tif in tifs:
        if page_digit in tif:
            return tif


def get_jpg(jpgs, page_digit):
    # jpg files have the form of /Img/Pg006.jpg'
    # addendum: jpg files can also have various resolution: Pg010.jpg, Pg010_120.jpg, Pg010_144.jpg
    if not jpgs:
        return None
    else:
        for jpg in jpgs:
            if page_digit in jpg:
                return jpg


def get_png(pngs, page_digit):
    # png files have the form of '/Img/Pg006_180.png' or '/Img/Pg006.png'
    if not pngs:
        return None
    else:
        # group by pages (there can be several images per pages)
        d = defaultdict(list)
        for i in pngs:
            elems = i.split("/", 1)
            d[elems[0]].append(elems[1])

        # take the image paths from the page we're interested in
        png_paths = d[page_digit]

        # there are several png
        if len(png_paths) > 1:
            # get pages with different resolutions (i.e. having "_": 'Img/Pg006_180.png')
            pngs_with_res = [x for x in png_paths if "_" in x]
            # return the highest resolution
            return pngs_with_res[-1]
        # there is only one png
        elif len(png_paths) == 1:
            return png_paths[0]
        else:
            return None


def get_imgdimensions(image_data):
    """ Returns image height and width"""
    img = cv.imdecode(np.frombuffer(image_data, np.uint8), 1)
    return tuple(img.shape[0:2])  # todo: check here which values to return