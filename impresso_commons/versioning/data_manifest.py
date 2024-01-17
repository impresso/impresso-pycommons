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
                                                 validate_format, clone_git_repo,
                                                 write_and_push_to_git, write_dump_to_fs)
from impresso_commons.versioning.data_statistics import DataStatistics
from impresso_commons.utils import Timer, timestamp
from impresso_commons.utils.utils import init_logger
from impresso_commons.utils.s3 import get_s3_resource, get_storage_options, get_boto3_bucket

logger = logging.getLogger(__name__)

GIT_REPO_SSH_URL = "git@github.com:impresso/impresso-data-release.git"
REPO_BRANCH_URL = "https://github.com/impresso/impresso-data-release/tree/{branch}"

IMPRESSO_STORAGEOPT = get_storage_options()

class DataManifest:

    def __init__(
        self, data_format: DataFormat|str, s3_input_bucket: str, 
        s3_output_bucket: str, git_repo: Repo, temp_folder: str,
        processing_stats: dict[str, DataStatistics]|None = None, 
        staging: bool|None = None
    ) -> None:

        # TODO check logger initialization
        #if logger is None:
        #    init_logger()
        self.format = validate_format(data_format)
        self.input_bucket_name = s3_input_bucket
        self.output_bucket_name = s3_output_bucket
        self.temp_folder = temp_folder
        self.branch = self._get_output_branch(staging)
        self.processing_stats = processing_stats
        
        # get code version used for processing.
        self.commit_hash = git_repo.head.commit
        # clone the data release repository locally
        self.out_repo = clone_git_repo(self.temp_folder, branch = self.branch)


        # initialize the dict containing the manifest data
        self.manifest_data = {}

    def add_processing_statistics(self, proc_stats: DataStatistics) -> None:
        if proc_stats.type == self.format and proc_stats.granularity == 'year':
            if self.processing_stats is not None:
                logger.debug(
                    f"`processing_stats`: {proc_stats} has been added to this "
                    " manifest but this attribute was already defined: "
                    f"{self.processing_stats}, updating the current value.")
            self.processing_stats = proc_stats
        else:
            logger.critical("Provided data statistics don't match with this "
                            "data manifest. Wrong data format or ganularity.")
            raise Exception

    def _get_output_branch(self, for_staging: bool|None) -> str:
        staging_out_bucket = 'staging' in self.output_bucket_name
        final_out_bucket = 'final' in self.output_bucket_name
        if for_staging is None:
            # if no argument was provided, use only the output bucket name to infer
            # if the stage is not in the bucket name, use staging branch by default
            for_staging = not (staging_out_bucket or final_out_bucket)
        # only pushing to master branch if `for_staging` was defined and False
        # or if 'final' was in the output s3 bucket and `for_staging` was None
        # --> `for_staging` overrides the result.
        return 'staging' if staging_out_bucket or for_staging else 'master'


    def get_prev_version_manifest(self):
        pass
        
        
    def read_prev_version_manifest(self) -> None:
        pass

    def read_input_data_manifest(self) -> None:
        pass

    def generate_modification_stats(self):
        pass

    def _manifest_filename_from_data(self) -> str:
        data_type = self.manifest_data['data_type']
        version_suffix = self.manifest_data['version'].replace('.', '-')

        return f"{data_type}_{version_suffix}.json"

    def _get_out_path_within_repo(
        self, folder_prefix: str = "data-processing-versioning"
    ) -> str:
        if self.format in ['canonical', 'rebuilt']:
            sub_folder = "data-preparation"
        else:
            sub_folder = "data-processing"

        return os.path.join(folder_prefix, sub_folder)

    def validate_and_export_manifest(self, write_in_git_folder: bool = False):
        # TODO add verification against JSON schema
        manifest_dump = json.dumps(self.manifest_data, indent=4)

        manifest_filename = self._manifest_filename_from_data()

        if not write_in_git_folder:
            # for debug purposes, write in temp file and not in git repo
            out_file_path = write_dump_to_fs(manifest_dump, self.temp_folder, 
                                             manifest_filename)
        else:
            # write file and push to git
            local_path_in_repo = self._get_out_path_within_repo()
            pushed, out_file_path = write_and_push_to_git(manifest_dump, 
                                                          self.out_repo, 
                                                          local_path_in_repo, 
                                                          manifest_filename, 
                                                          commit_msg = None)

        # upload to s3
        s3.upload(out_file_path, bucket_name=self.output_bucket_name)
        
        if not pushed:
            logger.critical(
                f"Push manifest to git manually using the file added on S3: "
                f"\ns3://{self.output_bucket_name}/{out_file_path}.")