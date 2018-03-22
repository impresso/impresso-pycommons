#!/usr/bin/python3
# coding: utf-8 


from enum import Enum
from collections import defaultdict
import os

__author__ = "maudehrmann"


class BoxStrategy(Enum):
    tif = "tif"
    png_highest = "png_highest"
    png_uniq = "png_uniq"
    jpg_uniq = "jpg_uniq"


class ImageCase(Enum):
    issues_wo_zip = 'issues_wo_zip'
    issues_with_corruptedzip = 'issues_with_corruptedzip'
    # issues fully covered with one img type
    issues_fully_covered_tifs = 'issues_fully_covered_tifs'
    issues_fully_covered_pngs = 'issues_fully_covered_pngs'
    issues_fully_covered_jpgs = 'issues_fully_covered_jpgs'
    issues_fully_covered_pngs_with_isolated = 'issues_fully_covered_pngs_with_isolated'
    # issues fully covered with mixed img type
    issues_mixed_covered_all = 'issues_mixed_covered_all'
    issues_mixed_covered_tif_png = 'issues_mixed_covered_tif_png'
    issues_mixed_covered_tif_jpg = 'issues_mixed_covered_tif_jpg'
    issues_mixed_covered_png_jpg = 'issues_mixed_covered_png_jpg'
    # partial
    issues_fully_covered = 'issues_fully_covered'
    # missing img
    issues_missing_pageimg = 'issues_missing_pageimg'


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
