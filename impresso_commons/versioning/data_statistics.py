"""This module contains the definition of a data statistics class.

A DataStatstics object should be instantiated during each processing step of 
the data preprocessing and augmentation of the Impresso project, and used to 
progressively count the number of elements modified or added by the processing.
"""

import logging
import os
import shutil
from abc import ABC, abstractmethod

#from impresso_commons.versioning.data_manifest import DataFormat
from impresso_commons.versioning.helpers import (DataFormat, validate_format,
                                                 validate_granularity)

logger = logging.getLogger(__name__)

POSSIBLE_ACTIONS = ['addition', 'modification']
POSSIBLE_GRANULARITIES = ['collection', 'title', 'year', 'issue']

class DataStatistics(ABC):

    def __init__(self, process_type: DataFormat | str, granularity: str, 
                 element: str = '', counts: dict[str, int] | None = None) -> None:

        self.format = validate_format(process_type)
        self.granularity = validate_granularity(granularity, for_stats=True)
        self.element = element
        self.count_keys = self._define_count_keys()

        if counts is not None and self._validate_count_keys(counts):
            self.counts = counts
        else:
            self.counts = self.init_counts() # defaultdict(int) ?

    # TODO if needed, define combination functions that allow summing/aggregating DataStats
            
    @abstractmethod         
    def _define_count_keys(self) -> list[str]:
        # define the count keys for this object
        pass 

    @abstractmethod
    def _validate_count_keys(self, new_counts: dict[str, int]) -> bool:
        # validate the keys of counts provided during instantiation
        pass

    def init_counts(self) -> dict[str, int]:
        # initialize a dict with all the keys associated to this object,
        # 0 for all values.
        return {k:0 for k in self.count_keys}

    def add_counts(self, new_counts: dict[str, int]) -> None:
        # ensure the incoming counts fits the current count keys
        if self._validate_count_keys(new_counts):
            for k, v in new_counts.items():
                self.counts[k] += v

    """def __add__(self, other):
        if self.format == other.format:
            sum_stats = DataStatistics(self.format, )
            for (k1, v1), (k2, v2) in zip(self.counts, other.counts):

        return DataStatistics(self.num+other.num)

    def __radd__(self,other):
        return MyNum(self.num+other)"""
                


class NewspaperStatistics(DataStatistics):

    possible_count_keys = [
        'titles',
        'issues',
        'pages', 
        'content_items_out',
        'ft_tokens',
        'images',
        'content_items_in',
        'embeddings_le',
        'ne_entities',
        'ne_mentions',
        'ne_links',
    ]

    def __init__(self, process_type: DataFormat | str, granularity: str, 
                 element: str = '', counts: dict[str, int] | None = None) -> None:

        super().__init__(process_type, granularity, element, counts)

    def _define_count_keys(self) -> list[str]:
        # TODO correct/update the count_keys
        start_index = int(self.granularity != 'collection')
        # all counts should have ['issues','pages', 'content_items', 'tokens']
        count_keys = self.possible_count_keys[start_index:5] 
        match self.format:
            case DataFormat.canonical:
                # add 'images'
                count_keys.append(self.possible_count_keys[5])
            case DataFormat.embeddings:
                # add 'embeddings'
                count_keys.append(self.format.value)
            case DataFormat.entities:
                # add 'entities'
                count_keys.append(self.format.value)
            case DataFormat.langident:
                # add 'languages'
                count_keys.append(self.possible_count_keys[7])
            case DataFormat.mentions:
                # add 'mentions'
                count_keys.append(self.format.value)
            case DataFormat.text_reuse:
                # add 'text_reuse_clusters'
                count_keys.append(self.possible_count_keys[-1])
            case DataFormat.topics:
                # add 'topics'
                count_keys.append(self.format.topics)
        return count_keys

    def _validate_count_keys(self, new_counts: dict[str, int]) -> bool:
        if not all(k in self.count_keys for k in new_counts.keys()):
            logger.warning(f"Provided value `counts`: {new_counts} has keys not"
                           f" present in `self.count_keys`: {self.count_keys}. "
                           "The counts provided will not be used.")
            return False
        elif not all(v>=0 for v in new_counts.values()):
            logger.warning("Provided count values are not all integers. "
                           "The counts provided will not be used.")
            return False
        # the provided counts were conforming
        return True
            