"""This module contains the definition of a manifest class.

A manifest object should be instantiated for each processing step of the data
preprocessing and augmentation of the Impresso project.  
"""

import logging
import os
import shutil

import boto3
import json
import dask.bag as db
import datetime
import pathlib
from git import Repo, Commit

from impresso_commons.path import parse_canonical_filename
from impresso_commons.path.path_fs import IssueDir
from impresso_commons.path.path_s3 import read_s3_issues
from impresso_commons.versioning.helpers import (DataFormat, read_manifest_contents, 
                                                 validate_format)
from impresso_commons.versioning.data_statistics import DataStatistics
from impresso_commons.utils import Timer, timestamp
from impresso_commons.utils.s3 import get_s3_resource

logger = logging.getLogger(__name__)

GIT_REPO_SSH_URL = "git@github.com:impresso/impresso-data-release.git"
GIT_REPO_HTTP_URL = "https://github.com/impresso/impresso-data-release.git"

class DataManifest:


    def __init__(self, data_format: DataFormat | str, s3_input_bucket: str, s3_output_bucket: str, 
                 git_repo: Repo, processing_stats: dict[str, DataStatistics]|None, temp_folder: str) -> None:
        self.format = validate_format(data_format)
        self.s3_input_bucket = s3_input_bucket
        self.s3_output_bucket = s3_output_bucket
        self.temp_folder = temp_folder

        if processing_stats:
            self.processing_stats = processing_stats
        else:
            self.processing_stats = None
        # get code version used for processing.
        self.commit_hash = git_repo.head.commit

    def add_processing_statistics(self, proc_stats: DataStatistics) -> None:
        if proc_stats.type == self.format and proc_stats.granularity == 'year':
            self.processing_stats = proc_stats
        else:
            logger.critical("Provided data statistics don't match with this "
                            "data manifest. Wrong data format or ganularity.")
            raise Exception

    def get_prev_version_manifest(self) :
        branch = 'staging' if 'staging' in s3_input_bucket else 'master'
        # TODO figure out how users can be identified for cloning/pulling/pushing code
        
    def read_prev_version_manifest(self) -> None:
        pass

    def read_input_data_manifest(self) -> None:
        pass

    def generate_modification_stats(self):
        pass

    def validate_and_export_manifest(self):
        pass