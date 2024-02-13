"""This module contains the definition of a manifest class.

A manifest object should be instantiated for each processing step of the data
preprocessing and augmentation of the Impresso project.  
"""

import logging
import os
import json

from typing import Any
from collections import defaultdict
from git import Repo

from impresso_commons.versioning.helpers import (
    DataStage,
    read_manifest_from_s3,
    validate_stage,
    clone_git_repo,
    write_and_push_to_git,
    write_dump_to_fs,
)
from impresso_commons.versioning.data_statistics import (
    DataStatistics,
    NewspaperStatistics,
)
from impresso_commons.utils.s3 import get_storage_options, upload

logger = logging.getLogger(__name__)

GIT_REPO_SSH_URL = "git@github.com:impresso/impresso-data-release.git"
REPO_BRANCH_URL = "https://github.com/impresso/impresso-data-release/tree/{branch}"

IMPRESSO_STORAGEOPT = get_storage_options()

VERSION_CHANGE = {
    "collection": "major",
    "title": "minor",
    "year": "patch",
    "issue": "patch",
}


class DataManifest:

    def __init__(
        self,
        data_stage: DataStage | str,
        s3_input_bucket: str,
        s3_output_bucket: str,
        git_repo: Repo,
        temp_folder: str,
        staging: bool | None = None,
        version_increment: str | None = None,
    ) -> None:

        # TODO check logger initialization
        # if logger is None:
        #    init_logger()
        self.stage = validate_stage(data_stage)  # update
        self.input_bucket_name = s3_input_bucket  # path to actual data partition
        self.output_bucket_name = s3_output_bucket
        self.temp_folder = temp_folder

        # attributes relating to GitHub
        self.branch = self._get_output_branch(staging)
        # get code version used for processing.
        self.commit_hash = git_repo.head.commit
        self.out_repo = None

        # init attributes of previous manifest
        self.prev_manifest_s3_path = None
        self.prev_v_manifest = None
        self.prev_version = None

        self.processing_stats = defaultdict(self.default_stats_value())  # TODO fix
        self.manifest_data = None

    def default_stats_value(self) -> NewspaperStatistics:
        return NewspaperStatistics(self.stage, "year")

    def add_processing_statistics(self, proc_stats: DataStatistics) -> None:
        # TODO review/correct
        if proc_stats.type == self.stage and proc_stats.granularity == "year":
            if self.processing_stats is not None:
                logger.debug(
                    "`processing_stats`: %s has been added to this manifest but this "
                    "attribute was already defined: %s, updating the current value.",
                    proc_stats,
                    self.processing_stats,
                )
            self.processing_stats = proc_stats
        else:
            logger.critical(
                "Provided data statistics don't match with this data manifest. "
                "Wrong data stage or granularity."
            )
            raise ValueError

    def _get_output_branch(self, for_staging: bool | None) -> str:
        staging_out_bucket = "staging" in self.output_bucket_name
        final_out_bucket = "final" in self.output_bucket_name
        if for_staging is None:
            # if no argument was provided, use only the output bucket name to infer
            # if the stage is not in the bucket name, use staging branch by default
            for_staging = not (staging_out_bucket or final_out_bucket)
        # only pushing to master branch if `for_staging` was defined and False
        # or if 'final' was in the output s3 bucket and `for_staging` was None
        # --> `for_staging` overrides the result.
        return "staging" if staging_out_bucket or for_staging else "master"

    def get_prev_version_manifest(self) -> dict[str, Any]:
        logger.debug("Reading the previous version of the manifest from S3.")
        (self.prev_manifest_s3_path, self.prev_v_manifest) = read_manifest_from_s3(
            self.output_bucket_name, self.stage
        )

        self.prev_version = self.prev_v_manifest["version"]

    def get_current_version(self) -> str:
        pass
        # modif_granularity = compute_update_granularty(self.processing_stats)
        # version_change =

    def get_input_data_manifest(self) -> None:
        pass

    def generate_modification_stats(self):
        pass

    def _manifest_filename_from_data(self) -> str:
        data_type = self.manifest_data["data_type"]
        version_suffix = self.manifest_data["version"].replace(".", "-")

        return f"{data_type}_{version_suffix}.json"

    def _get_out_path_within_repo(
        self, folder_prefix: str = "data-processing-versioning"
    ) -> str:
        if self.stage in ["canonical", "rebuilt"]:
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
            out_file_path = write_dump_to_fs(
                manifest_dump, self.temp_folder, manifest_filename
            )
        else:
            # write file and push to git
            local_path_in_repo = self._get_out_path_within_repo()
            pushed, out_file_path = write_and_push_to_git(
                manifest_dump,
                self.out_repo,
                local_path_in_repo,
                manifest_filename,
                commit_msg=None,
            )
            if not pushed:
                logger.critical(
                    "Push manifest to git manually using the file added on S3: "
                    "\ns3://%s/%s.",
                    self.output_bucket_name,
                    out_file_path,
                )

        # upload to s3
        upload(out_file_path, bucket_name=self.output_bucket_name)

    def compute(self) -> None:
        # function that will perform all the logic to construct the manifest
        # (similarly to NewsPaperPages)

        # clone the data release repository locally
        self.out_repo = clone_git_repo(self.temp_folder, branch=self.branch)

        # initialize the dict containing the manifest data
        self.manifest_data = {}

        #### IMPLEMENT LOGIC AND FILL MANIFEST DATA
