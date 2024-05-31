"""This module contains the definition of a data statistics class.

A DataStatstics object should be instantiated during each processing step of 
the data preprocessing and augmentation of the Impresso project, and used to 
progressively count the number of elements modified or added by the processing.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Union, Self, Optional

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
    """Count statistics computed on a specific portion and granularity of the data.

    Args:
        data_stage (DataStage | str): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str, optional): The specific element associated with the statistics.
            Defaults to "" (empty string).
        counts (dict[str, int | dict[str, int]] | None, optional): Initial counts for
            statistics. Defaults to None.

    Attributes:
        stage (DataStage): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str): The specific element associated with the statistics.
        count_keys (list[str]): The count keys for these statistics.
        counts (dict[str, int | dict[str, int]]): The count statistics computed on the
            specific data, can include frequency dicts.
    """

    def __init__(
        self,
        data_stage: Union[DataStage, str],
        granularity: str,
        element: Optional[str] = None,
        counts: Union[dict[str, Union[int, dict[str, int]]], None] = None,
    ) -> None:

        self.stage = validate_stage(data_stage)
        self.granularity = validate_granularity(granularity)
        self.element = element
        self.count_keys = self._define_count_keys()

        if counts is not None and self._validate_count_keys(counts):
            self.counts = counts
        else:
            logger.debug("Initializing counts to 0 for %s.", self.element)
            self.counts = self.init_counts()

    @abstractmethod
    def _define_count_keys(self) -> list[str]:
        """Define the count keys for these specific statistics."""

    @abstractmethod
    def _validate_count_keys(
        self, new_counts: dict[str, Union[int, dict[str, int]]]
    ) -> bool:
        """Validate the keys of new counts provided against defined count keys."""

    def init_counts(self) -> dict[str, Union[int, dict[str, int]]]:
        """Initialize a dict with all the keys associated to this object.

        Returns:
            dict[str, int | dict[str, int]]: A dict with all defined keys, and values
                initialized to 0 (or to empty frequency dicts).
        """
        return {k: 0 if "fd" not in k else {} for k in self.count_keys}

    def add_counts(
        self, new_counts: dict[str, Union[int, dict[str, int]]], replace: bool = False
    ) -> bool:
        """Add new counts to the existing counts if the new keys are validated.

        Args:
            new_counts (dict[str, int | dict[str, int]]): New counts to be added.

        Returns:
            bool: True if the counts were valid and could be added, False otherwise.
        """
        if self._validate_count_keys(new_counts):
            if replace:
                logger.debug(
                    "Replacing the counts by %s for %s. This will erase previous counts.",
                    new_counts,
                    self.element,
                )
                self.counts = new_counts
            else:
                for k, v in new_counts.items():
                    if k.endswith("_fd"):
                        # some fields can be frequency dicts
                        for v_k, v_f in v.items():
                            self.counts[k][v_k] = (
                                v_f
                                if v_k not in self.counts[k]
                                else self.counts[k][v_k] + v_f
                            )
                    else:
                        self.counts[k] += v
            return True

        return False

    def pretty_print(
        self, modif_date: Optional[str] = None, include_counts: bool = False
    ) -> dict[str, Any]:
        """Generate a dict representation of these statistics to add to a json.

        These stats are agnostic to the type of statistics they represent so the values
        of `self.counts` are excluded by default, to be included in child classes.
        The modification date can also be included (when granularity='year')

        Args:
            modif_date (Optional[str], optional): Last modification date of the
                corresponding elements. Defaults to None.
            include_counts (bool, optional): Whether to include the current counts with
                key "stats". Defaults to False.

        Returns:
            dict[str, Any]: A dict with the general information about these statistics.
        """
        stats_dict = {
            "stage": self.stage.value,
            "granularity": self.granularity,
        }

        # no element for the overall stats
        if self.granularity != "corpus":
            if self.element is not None:
                stats_dict["element"] = self.element
            else:
                logger.warning("Missing the element when pretty-printing!")

        # If a modification date is provided, add it.
        if modif_date is not None:
            stats_dict["last_modification_date"] = modif_date
            if self.granularity != "year":
                # if the granularity is not year, log a warning (unexpected behavior)
                logger.warning(
                    "'last_modification_date' field was added although granularity is %s",
                    self.granularity,
                )

        if include_counts:
            stats_dict["stats"] = {
                k: (
                    v
                    if "fd" not in k
                    else {v_k: v_f for v_k, v_f in v.items() if v_f > 0}
                )
                for k, v in self.counts.items()
                if "_fd" in k or v > 0
            }

        return stats_dict

    @abstractmethod
    def same_counts(self, other_stats: Union[dict[str, Any], Self]) -> bool:
        """Given another dict of stats, check whether the values are the same."""


class NewspaperStatistics(DataStatistics):
    """Count statistics computed on a specific portion and granularity of the data.

    Args:
        data_stage (DataStage | str): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str, optional): The specific element associated with the statistics.
            Defaults to "" (empty string).
        counts (dict[str, int] | None, optional): Initial counts for statistics.
            Defaults to None.

    Attributes:
        stage (DataStage): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str): The specific element associated with the statistics.
        count_keys (list[str]): The count keys for these statistics.
        counts (dict[str, int]): The count statistics computed on the specific data.
    """

    # All possible count keys for newspaper data.
    possible_count_keys = [
        "titles",
        "issues",
        "pages",
        "content_items_out",
        "ft_tokens",
        "images",
        "content_items_in",
        "ne_mentions",
        "ne_entities",
        "embeddings_el",
        "topics",
        "lang_fd",  # '_fd' suffix signifies a frenquency dict
        "text_reuse_clusters",
        "text_reuse_passages",
    ]

    def _define_count_keys(self) -> list[str]:
        """Define the count keys to use for these specific statistics.

        TODO correct/update the count_keys

        Returns:
            list[str]: The count keys for this specific stage and granularity.
        """
        start_index = int(self.granularity != "corpus")
        # all counts should have 'content_items_out'
        count_keys = [self.possible_count_keys[3]]
        # add 'issues' and 'titles' (only if corpus granularity)
        count_keys.extend(self.possible_count_keys[start_index:2])

        match self.stage:
            case DataStage.CANONICAL:
                # add 'pages' and 'images'
                count_keys.append(self.possible_count_keys[2])
                count_keys.append(self.possible_count_keys[5])
                # keys: 'content_items_out', 'titles', 'issues', 'pages', 'images'
            case DataStage.REBUILT:
                # add 'ft_tokens'
                count_keys.append(self.possible_count_keys[4])
                # keys: 'content_items_out', 'titles', 'issues', 'ft_tokens'
            case DataStage.EMBEDDINGS:
                # add 'embeddings'
                count_keys.append(self.stage.value)
                # TODO update
                # keys: 'content_items_out', 'titles', 'issues', 'embeddings'
            case DataStage.ENTITIES:
                # add 'ne_entities', 'ne_mentions'
                count_keys.extend(self.possible_count_keys[7:9])
                # keys: 'content_items_out', 'titles', 'issues', 'ne_entities', 'ne_mentions'
            case DataStage.PASSIM:
                # add 'ft_tokens'
                count_keys.append(self.possible_count_keys[4])
                # keys: 'content_items_out', 'titles', 'issues', 'ft_tokens'
            case DataStage.LANGIDENT:
                # add 'images', 'lang_fd'
                count_keys.append(self.possible_count_keys[5])
                count_keys.append(self.possible_count_keys[11])
                # keys: 'content_items_out', 'titles', 'issues', 'pages', 'lang_fd'
            case DataStage.TEXT_REUSE:
                # add 'text_reuse_clusters' and 'text_reuse_passages'
                count_keys.append(self.possible_count_keys[12])
                count_keys.append(self.possible_count_keys[13])
                # keys: 'content_items_out', 'titles', 'issues', 'text_reuse_clusters', 'text_reuse_passages'
            case DataStage.TOPICS:
                # add 'topics'
                # TODO update
                count_keys.append(self.possible_count_keys[10])
            case DataStage.MYSQL_CIS:
                # add 'pages'
                count_keys.append(self.possible_count_keys[2])
                # keys: 'content_items_out', 'titles', 'issues', 'pages'

        # For case DataStage.SOLR_TEXT, all keys are already added.
        #   keys: 'content_items_out', 'titles', 'issues'
        return count_keys

    def _validate_count_keys(
        self, new_counts: dict[str, Union[int, dict[str, int]]]
    ) -> bool:
        """Validate the keys of new counts provided against defined count keys.

        Valid new counts shouldn't have keys absent from the defined `attr:count_keys`
        or non-integer values.

        Args:
            new_counts (dict[str, int | dict[str, int]]): New counts to validate

        Returns:
            bool: True if `new_counts` are valid, False otherwise.
        """
        if not all(k in self.count_keys for k in new_counts.keys()):
            warn_msg = (
                f"Provided value `counts`: {new_counts} has keys not present in "
                f"`count_keys`: {self.count_keys}. The counts provided won't be used."
            )
            logger.error(warn_msg)
            return False

        if not all(
            v >= 0 if "fd" not in k else all(fv > 0 for fv in v.values())
            for k, v in new_counts.items()
        ):
            logger.error(
                "Provided count values are not all integers and will not be used."
            )
            return False

        # the provided counts were conforming
        return True

    def pretty_print(
        self, modif_date: Optional[str] = None, include_counts: bool = True
    ) -> dict[str, Any]:
        """Generate a dict representation of these statistics to add to a json.

        Args:
            modif_date (Optional[str], optional): Last modification date of the
                corresponding elements. Defaults to None.
            include_counts (bool, optional): Whether to include the current newspaper
                counts with key "nps_stats". Defaults to True.

        Returns:
            dict[str, Any]: A dict representation of these statistics.
        """
        stats_dict = super().pretty_print(modif_date=modif_date)
        # add the newspaper stats
        if include_counts:
            stats_dict["nps_stats"] = {
                k: (
                    v
                    if "_fd" not in k
                    else {v_k: v_f for v_k, v_f in v.items() if v_f > 0}
                )
                for k, v in self.counts.items()
                if "_fd" in k or v > 0
            }

        return stats_dict

    def same_counts(self, other_stats: Union[dict[str, Any], Self]) -> bool:
        """Given another dict of stats, check whether the values are the same.

        Args:
            other_stats (Union[dict[str, Any], Self]): Dict with pretty-printed
                newspaper statistics or other NewspaperStatistics object.

        Returns:
            bool: True if the values for the various fields of `nps_stats` where the
                same, False otherwise.
        """
        if isinstance(other_stats, NewspaperStatistics):
            other_stats = other_stats.pretty_print()

        self_stats = self.pretty_print()
        return self_stats["nps_stats"] == other_stats["nps_stats"]
