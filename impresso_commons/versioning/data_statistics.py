"""This module contains the definition of a data statistics class.

A DataStatstics object should be instantiated during each processing step of 
the data preprocessing and augmentation of the Impresso project, and used to 
progressively count the number of elements modified or added by the processing.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

# from impresso_commons.versioning.data_manifest import DataStage
from impresso_commons.versioning.helpers import (
    DataStage,
    validate_stage,
    validate_granularity,
)

logger = logging.getLogger(__name__)

POSSIBLE_ACTIONS = ["addition", "modification"]
POSSIBLE_GRANULARITIES = ["corpus", "title", "year"]


class DataStatistics(ABC):

    def __init__(
        self,
        data_stage: DataStage | str,
        granularity: str,
        element: str = "",
        counts: dict[str, int] | None = None,
    ) -> None:

        self.stage = validate_stage(data_stage)
        self.granularity = validate_granularity(granularity, for_stats=True)
        self.element = element
        self.count_keys = self._define_count_keys()

        if counts is not None and self._validate_count_keys(counts):
            self.counts = counts
        else:
            self.counts = self.init_counts()  # defaultdict(int) ?

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
        return {k: 0 for k in self.count_keys}

    def add_counts(self, new_counts: dict[str, int]) -> None:
        # ensure the incoming counts fits the current count keys
        if self._validate_count_keys(new_counts):
            for k, v in new_counts.items():
                self.counts[k] += v

    """def __add__(self, other: Self):
        if self.format == other.format:
            sum_stats = DataStatistics(self.format, )
            for (k1, v1), (k2, v2) in zip(self.counts, other.counts):

        return DataStatistics(self.num+other.num)

    def __radd__(self,other):
        return MyNum(self.num+other)"""

    def pretty_print(self, add_counts: bool = False) -> dict[str, Any]:
        """Generate a dict representation of these statistics to add to a json.

        These stats are agnostic to the type of statistics they represent so the values
        of `self.counts` are excluded by default, to be included in child classes.

        Args:
            add_counts (bool, optional): Whether to include the current counts with key
                "stats". Defaults to False.

        Returns:
            dict[str, Any]: A dict with the general information about these statistics.
        """
        stats_dict = {
            "stage": self.stage.value,
            "granularity": self.granularity,
            "element": self.element,
        }

        if add_counts:
            stats_dict["stats"] = {k: v for k, v in self.counts.items() if v > 0}

        # no element for the overall stats
        if stats_dict["granularity"] == "corpus":
            del stats_dict["element"]

        return stats_dict


class NewspaperStatistics(DataStatistics):

    possible_count_keys = [
        "titles",
        "issues",
        "pages",
        "content_items_out",
        "ft_tokens",
        "images",
        "content_items_in",
        "ne_entities",
        "ne_mentions",
        "ne_links",
        "embeddings_el",
        "topics",
    ]

    def _define_count_keys(self) -> list[str]:
        # TODO correct/update the count_keys
        start_index = int(self.granularity != "corpus")
        # all counts should have 'content_items_out'
        count_keys = [self.possible_count_keys[3]]
        match self.stage:
            case DataStage.canonical:
                # add 'titles', 'issues', 'pages' and 'images'
                count_keys.extend(self.possible_count_keys[start_index:3])
                count_keys.append(self.possible_count_keys[5])
            case DataStage.embeddings:
                # add 'embeddings'
                count_keys.append(self.stage.value)
            case DataStage.entities:
                # add 'entities'
                count_keys.append(self.stage.value)
            case DataStage.langident:
                # add 'languages'
                count_keys.append(self.possible_count_keys[7])
            case DataStage.mentions:
                # add 'mentions'
                count_keys.append(self.stage.value)
            case DataStage.text_reuse:
                # add 'text_reuse_clusters'
                count_keys.append(self.possible_count_keys[-1])
            case DataStage.topics:
                # add 'topics'
                count_keys.append(self.stage.topics)
        return count_keys

    def _validate_count_keys(self, new_counts: dict[str, int]) -> bool:
        if not all(k in self.count_keys for k in new_counts.keys()):
            warn_msg = (
                f"Provided value `counts`: {new_counts} has keys not present in "
                f"`count_keys`: {self.count_keys}. The counts provided won't be used."
            )
            logger.warning(warn_msg)
            return False

        if not all(v >= 0 for v in new_counts.values()):
            logger.warning(
                "Provided count values are not all integers and will not be used."
            )
            return False

        # the provided counts were conforming
        return True

    def pretty_print(self, add_counts: bool = True) -> dict[str, Any]:
        """Generate a dict representation of these statistics to add to a json.

        Args:
            add_counts (bool, optional): Whether to include the current newspaper
                counts with key "nps_stats". Defaults to True.

        Returns:
            dict[str, Any]: A dict representation of these statistics.
        """
        stats_dict = super().pretty_print()
        # add the newspaper stats
        if add_counts:
            stats_dict["nps_stats"] = {k: v for k, v in self.counts.items() if v > 0}

        return stats_dict
