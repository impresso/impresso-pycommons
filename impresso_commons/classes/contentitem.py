#!/usr/bin/env python
# coding: utf-8

import datetime
import json
import os
from enum import Enum


# TODO: implement
def index_coordinates(pages):
    """Creates an index of coordinates by text offsets.

    Gets called by `self.from_json()`.
    """
    coords = []
    for page in pages:

        for token in page['t']:
            item = {
                'page_id': page['id'],
                'coords': token['c'],
                'start_offset': token['s'],
                'end_offset': token['s'] + token['l']
            }
            if "hy1" in token:
                item['hy1'] = token['hy1']
            if "hy2" in token:
                item['hy2'] = token['hy2']
            coords.append(item)

    return coords

class ContentItemCase(Enum):
    FULL = "FULL"  # all info
    TEXT = "TEXT"  # min info + text
    LIGHT = "LIGHT"  # min info

class ContentItem:
    """
    Class which represents an impresso (rebuilt) content item.
    TODO: complement
    :ivar str id: canonical content item id
    :ivar str lg:
    :ivar str type:
    :ivar datetime date:
    :ivar str journal:
    :ivar str s3v:
    :ivar str fulltext:
    :ivar dict text_offsets: pages/regions/paragraphs/lines
    """

    def __init__(self, ci_id, lg, tp):
        """Constructor"""
        self.id = ci_id
        self.lg = lg
        self.type = tp
        self.date = self.build_date(ci_id)
        self.journal = self.build_journal(ci_id)
        self._text_offsets = {}
        self.__coordinates = []

    @staticmethod
    def build_date(ci_id):
        tmp = ci_id.split("-")
        return datetime.date(int(tmp[1]), int(tmp[2]), int(tmp[3]))

    @staticmethod
    def build_journal(ci_id):
        return ci_id.split("-")[0]

    @property
    def title(self):
        return self.__title

    @title.setter
    def title(self, value):
        self.title = value

    @property
    def lines(self):
        return self.__lines

    @lines.setter
    def lines(self, value):
        self.__lines = value

    @property
    def paragraphs(self):
        return self.__paragraphs

    @paragraphs.setter
    def paragraphs(self, value):
        self.__paragraphs = value

    @property
    def pages(self):
        return self.__pages

    @pages.setter
    def pages(self, value):
        self.__pages = value

    @property
    def regions(self):
        return self.__regions

    @regions.setter
    def regions(self, value):
        self.__regions = value

    @property
    def fulltext(self):
        return self.__fulltext

    @fulltext.setter
    def fulltext(self, value):
        self.fulltext = value

    @staticmethod
    def from_json(path=None, data=None, case=ContentItemCase.LIGHT):
        """Loads an instance of `ContentItem` from a JSON file.
        :param str path: path to a json file
        :param dict data: content item information
        :param enum case: content item configuration via `ContentItemCase`
        (LIGHT/TEXT/FULL)
        """

        assert data is not None or path is not None

        if data is not None:
            lang = None if "lg" not in data else data['lg']
            doc = ContentItem(data['id'], lang, data['tp'])
            doc.case = case

            if case == ContentItemCase.TEXT or case == ContentItemCase.FULL:
                doc.__title = data['t'] if 't' in data else None
                doc.__fulltext = data['ft'] if 'ft' in data else None

            if case == ContentItemCase.FULL:
                doc.__lines = data['lb'] if 'lb' in data else None
                doc.__paragraphs = data['pb'] if 'pb' in data else None
                doc.__regions = data['rb'] if 'pb' in data else None
                doc.__pages = data['ppreb'] if 'ppreb' in data else None
                doc.__coordinates = index_coordinates(doc.__pages)

            return doc
        elif path is not None:
            return


    def to_json(self, path, case=ContentItemCase.LIGHT):
        data = {
            "id": self.id,
            "lg": self.lg,
            "tp": self.type
        }

        if case == ContentItemCase.TEXT or case == ContentItemCase.FULL:
            data['t'] = self.__title
            data['ft'] = self.__fulltext

        if case == ContentItemCase.FULL:
            data['lb'] = self.__lines
            data['pb'] = self.__paragraphs
            data['rb'] = self.__regions
            data['ppreb'] = self.__pages

        json_output_path = os.path.join(path, f'{self.id}.json')
        try:
            with open(json_output_path, 'w') as out_file:
                json.dump(data, out_file)
            return True
        except Exception as e:
            print(e)
            return False


    def get_coordinates(self, start_offset, end_offset):
        """Enables access to image coordinates by means of text offsets.

        returns a dictionary like
        {
            'page_id': '....',
            'coordinates': [x,y,w,h]
        }
        """
        return list(filter(
            lambda c: c['start_offset'] >= start_offset and c['end_offset'] <= end_offset,
            self.__coordinates
        ))


    def __str__(self):
        s = f'{self.__class__.__name__}:\n\t' \
                f'ci_case={self.case}\n\t' \
                f'ci_id={self.id}\n\t' \
                f'ci_lg={self.lg}\n\t' \
                f'ci_type={self.type}\n\t' \
                f'ci_date={self.date}\n\t' \

        if self.case == ContentItemCase.TEXT \
                or self.case == ContentItemCase.FULL:
            s = s + f'ci_fulltext={self.fulltext}\n\t' \
                   f'ci_title={self.title}\n\t' \

        return s
