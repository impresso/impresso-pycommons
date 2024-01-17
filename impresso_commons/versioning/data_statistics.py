"""This module contains the definition of a data statistics class.

A DataStatstics object should be instantiated during each processing step of 
the data preprocessing and augmentation of the Impresso project, and used to 
progressively count the number of elements modified or added by the processing.
"""

import logging
import os
import shutil
#from impresso_commons.versioning.data_manifest import DataFormat
from impresso_commons.versioning.helpers import (DataFormat, validate_format,
                                                 validate_granularity)

logger = logging.getLogger(__name__)

POSSIBLE_ACTIONS = ['addition', 'modification']

class DataStatistics:

    def __init__(self, process_type: DataFormat | str, granularity: str, addition: bool) -> None:

        self.type = validate_format(process_type)
        self.granularity = validate_granularity(granularity, for_stats=True)
        self.action_type = 'addition' if addition else 'modification'
