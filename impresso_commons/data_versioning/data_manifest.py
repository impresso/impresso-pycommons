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

from enum import StrEnum

from impresso_commons.path import parse_canonical_filename
from impresso_commons.path.path_fs import IssueDir
from impresso_commons.path.path_s3 import read_s3_issues
from impresso_commons.text.helpers import (read_issue_pages, rejoin_articles)
from impresso_commons.utils import Timer, timestamp
from impresso_commons.utils.s3 import get_s3_resource

logger = logging.getLogger(__name__)

GIT_REPO_URL = "https://github.com/impresso/impresso-data-release"

class DataFormat(StrEnum):

    canonical = 'canonical'
    rebuilt = 'rebuilt'
    embeddings = 'embeddings'
    entities = 'entities'
    langident = 'langident'
    mentions = 'mentions'
    text_reuse = 'text-reuse'
    topics = 'topics'

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_ 


class DataManifest:


    def __init__(self, data_format: DataFormat | str, s3_input_bucket: str, s3_output_bucket: str, git_repo: Repo) -> None:
        self.format = self.validate_format(data_format)
        self.s3_input_bucket = s3_input_bucket
        self.s3_output_bucket = s3_output_bucket
        self.commit_hash = git_repo.head.commit

    
    def validate_format(self, data_format: str) -> DataFormat | None:
        try:
            return DataFormat[data_format]
        except ValueError as e:
            logger.critical(f"{e} \nProvided data format '{data_format}'"
                            " is not a valid data format.")
            raise e

    def get_last_manifest(self) :
        branch = 'staging' if 'staging' in s3_input_bucket else 'master'
        # TODO figure out how users can be identified for cloning/pulling/pushing code
        