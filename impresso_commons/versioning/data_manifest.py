"""This module contains the definition of a manifest class.

A manifest object should be instantiated for each processing step of the data
preprocessing and augmentation of the Impresso project.  
"""

import logging
import os
import json
import copy

from typing import Any
from git import Repo
from time import strftime

from impresso_commons.versioning.helpers import (
    DataStage,
    read_manifest_from_s3,
    validate_stage,
    clone_git_repo,
    write_and_push_to_git,
    write_dump_to_fs,
    get_head_commit_url,
    increment_version,
    validate_version,
    init_media_info,
    media_list_from_mft_json,
)
from impresso_commons.versioning.data_statistics import NewspaperStatistics
from impresso_commons.utils.s3 import get_storage_options, upload_to_s3

logger = logging.getLogger(__name__)

GIT_REPO_SSH_URL = "git@github.com:impresso/impresso-data-release.git"
REPO_BRANCH_URL = "https://github.com/impresso/impresso-data-release/tree/{branch}"

IMPRESSO_STORAGEOPT = get_storage_options()


class DataManifest:

    def __init__(
        self,
        data_stage: DataStage | str,
        s3_input_bucket: str,
        s3_output_bucket: str,  # including partition
        git_repo: Repo,
        temp_dir: str,
        staging: bool | None = None,
        # to directly provide the next version
        new_version: str | None = None,
        # to indicate if patch in later stages
        is_patch: bool | None = False,
        # to indcate patch in canonical/rebuilt
        patched_fields: dict[str, list[str]] | list[str] | None = None,
    ) -> None:

        # TODO check logger initialization
        # if logger is None:
        #    init_logger()

        # TODO modif for Solr (no output bucket)

        # TODO remove all non-necessary attributes
        self.stage = validate_stage(data_stage)  # update
        self.input_bucket_name = s3_input_bucket

        # s3_output_bucket is the path to actual data partition
        s3_output_bucket = s3_output_bucket.replace("s3://", "")
        if "/" in s3_output_bucket:
            self.output_bucket_name = s3_output_bucket.split("/")[0]
            self.output_s3_partition = "/".join(s3_output_bucket.split("/")[1:])
        else:
            # for data preparation, the manifest is at the top level of the bucket
            self.output_bucket_name, self.output_s3_partition = s3_output_bucket, None

        self.temp_dir = temp_dir
        self.notes = None

        # attributes relating to GitHub
        self.branch = self._get_output_branch(staging)
        # get code version used for processing.
        self.commit_url = get_head_commit_url(git_repo)

        # init attributes of previous manifest
        self._prev_mft_s3_path = None
        # self._prev_v_mft = None
        self._prev_v_mft_yearly_stats = None
        self.prev_version = None

        # init attribute of input_manifest
        self.input_manifest_s3_path = None

        # if user already knows the target version
        self.version = validate_version(new_version)
        # if update is a patch, patched fields should be provided
        # either as list of fields or dict mapping each media to its patched fields
        self.patched_fields = patched_fields
        self.is_patch = is_patch

        # example of 1 yearly stats format to gather the necessary keys
        self._eg_yearly_stats = NewspaperStatistics(self.stage, "year")

        # dict mapping from title to year to np_stat,
        # where the statistics will be aggregated during the processing
        self._processing_stats = {}
        self.manifest_data = {}
        self._generation_date = None

        logger.info("DataManifest for %s successfully initialized.", self.stage)

    @property
    def _input_stage(self) -> DataStage:
        return (
            DataStage.canonical
            if self.stage == DataStage.rebuilt
            else DataStage.rebuilt
        )

    @property
    def _manifest_filename(self) -> str:
        return f"{self.stage.value}_{self.version.replace('.', '-')}.json"

    @property
    def output_mft_s3_path(self) -> str:
        if self.output_s3_partition is not None:
            s3_path = os.path.join(self.output_s3_partition, self._manifest_filename)
        else:
            s3_path = self._manifest_filename

        if self._prev_mft_s3_path is not None:
            assert (
                self._prev_mft_s3_path.split(f"/{self.stage.value}_v")[0]
                == self.output_s3_partition
            ), "Mismatch between previous & current version of manifest."

        return os.path.join("s3:/", self.output_bucket_name, s3_path)

    def _get_output_branch(self, for_staging: bool | None) -> str:
        # TODO recheck logic
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

    def _get_prev_version_manifest(self) -> dict[str, Any] | None:
        logger.debug("Reading the previous version of the manifest from S3.")
        (self._prev_mft_s3_path, prev_v_mft) = read_manifest_from_s3(
            self.output_bucket_name, self.stage, self.output_s3_partition
        )

        if self._prev_mft_s3_path is None or prev_v_mft is None:
            logger.info(
                "No existing previous version of this manifest. Version will be v0.0.1"
            )
            self.prev_version = "v0.0.0"

        else:
            self.prev_version = prev_v_mft["version"]

        return prev_v_mft

    def _get_current_version(self, addition: bool = False) -> str:
        """Get the current version of manifest for the data stage.

        Versions are of format vM.m.p (=Major.minor.patch), where:
        - M changes in the case of additions to the collection (new “title-year” keys).
        - m when modifications are made to existing data (keeping the same ‘title-year’
            keys,by re-ingestion (not field-specific modifications).
        - p when a patch or small fix is made, modifying one or more fields for several
            titles, leaving the rest of the data unchanged.
            When `attr:self.is_patch` is True or `attr:patched_fields` is defined.

        Args:
            addition (bool, optional): Whether new data was added during the processing.
                Defaults to False.

        Returns:
            str: The current version string.
        """
        if self.prev_version is None:
            # First manifest for this data stage.
            return "v0.0.1"

        if addition:
            # any new title-year pair was added during processing
            return increment_version(self.prev_version, "major")

        if self.is_patch or self.patched_fields is not None:
            # processing is a patch
            return increment_version(self.prev_version, "patch")

        # modifications were made by re-ingesting/re-generating the data, not patching
        return increment_version(self.prev_version, "minor")

    def _get_input_data_overall_stats(self) -> list[dict[str, Any]]:
        if self.stage != DataStage.canonical:
            logger.debug("Reading the input data's manifest from S3.")

            # only the rebuilt uses the canonical as input
            (self.input_manifest_s3_path, input_v_mft) = read_manifest_from_s3(
                self.input_bucket_name, self._input_stage
            )

            assert self.input_manifest_s3_path == input_v_mft["mft_s3_path"]

            # fetch the overall statistics from the input data (it's a list!)
            return input_v_mft["overall_statistics"]

        # The first stage of the data is canonical
        return []

    def _get_out_path_within_repo(
        self,
        folder_prefix: str = "data-processing-versioning",
        stage: DataStage | None = None,
    ) -> str:
        # TODO add data-indexation for SOLR
        stage = stage if stage is not None else self.stage
        if stage in ["canonical", "rebuilt"]:
            sub_folder = "data-preparation"
        else:
            sub_folder = "data-processing"

        return os.path.join(folder_prefix, sub_folder)

    def validate_and_export_manifest(self, out_repo: Repo | None = None) -> bool:
        # TODO add verification against JSON schema
        manifest_dump = json.dumps(self.manifest_data, indent=4)

        mft_filename = self._manifest_filename

        # if out_repo is None, in debug mode
        if out_repo is not None:
            # write file and push to git
            local_path_in_repo = self._get_out_path_within_repo()
            pushed, out_file_path = write_and_push_to_git(
                manifest_dump,
                out_repo,
                local_path_in_repo,
                mft_filename,
                commit_msg=None,
            )
            if not pushed:
                logger.critical(
                    "Push manifest to git manually using the file added on S3: \ns3://%s/%s.",
                    self.output_bucket_name,
                    out_file_path,
                )
        else:
            # for debug purposes, write in temp dir and not in git repo
            out_file_path = write_dump_to_fs(manifest_dump, self.temp_dir, mft_filename)

        if self.output_s3_partition is not None:
            # add the path within the bucket (partition) to the manifest file
            mft_filename = os.path.join(self.output_bucket_name, mft_filename)

        return upload_to_s3(out_file_path, mft_filename, self.output_bucket_name)

    def get_count_keys(self) -> list[str]:
        return self._eg_yearly_stats.count_keys

    def init_yearly_count_dict(self) -> dict[str, int]:
        return self._eg_yearly_stats.init_counts()

    def _log_failed_action(self, title: str, year: str, action: str) -> None:

        failed_note = f"{title}-{year}: {action} provided counts failed (invalid)."

        logger.warning(" ".join([failed_note, "Adding information to notes."]))
        self.append_to_notes(failed_note, to_start=False)

    def _init_yearly_stats(
        self, title: str, year: str, counts: dict[str, int]
    ) -> tuple[NewspaperStatistics, bool]:
        elem = f"{title}-{year}"
        np_stats = NewspaperStatistics(self.stage, "year", elem, counts=counts)
        success = True
        # if the created count keys
        if np_stats.counts != counts:
            success = False
            self._log_failed_action(title, year, "initializing with")

        return np_stats, success

    def _modify_processing_stats(
        self, title: str, year: str, counts: dict[str, int], adding: bool = True
    ) -> bool:
        # check if title/year pair is already in processing stats
        if title in self._processing_stats:
            if year in self._processing_stats[title]:
                success = self._processing_stats[title][year].add_counts(
                    counts, replace=(not adding)
                )

                if not success:
                    action = "adding" if adding else "replacing with"
                    self._log_failed_action(title, year, action)
                # notify user of outcome
                return success
        else:
            self._processing_stats[title] = {}

        # initialize new statistics for this title-year pair:
        self._processing_stats[title][year], success = self._init_yearly_stats(
            title, year, counts
        )

        return success

    # TODO replace by add & replace with different parameters set to None by default?
    def add_by_ci_id(self, ci_id: str, counts: dict[str, int]) -> bool:
        title, year = ci_id.split("-")[0:2]
        return self._modify_processing_stats(title, year, counts)

    def add_by_title_year(self, title: str, year: str, counts: dict[str, int]) -> bool:
        return self._modify_processing_stats(title, year, counts)

    def replace_by_ci_id(self, ci_id: str, counts: dict[str, int]) -> bool:
        title, year = ci_id.split("-")[0:2]
        return self._modify_processing_stats(title, year, counts, adding=False)

    def replace_by_title_year(
        self, title: str, year: str, counts: dict[str, int]
    ) -> bool:
        return self._modify_processing_stats(title, year, counts, adding=False)

    def append_to_notes(self, contents: str, to_start: bool = True) -> None:
        if self.notes is None:
            self.notes = contents
        else:
            new_notes = [contents, self.notes] if to_start else [self.notes, contents]
            self.notes = "\n".join(new_notes)

    def new_media(self, title: str) -> dict[str, Any]:
        # adding a new media means by default addition update type and title update level.
        logger.info("Creating new media dict for %s.", title)
        media = {
            "media_title": title,
            "last_modification_date": "self._generation_date",
        }
        media.update(init_media_info(fields=self.patched_fields))
        media.update(
            {
                "code_git_commit": "self.commit_url",
                "media_statistics": [],
                "stats_as_dict": {},
            }
        )

        return media

    def update_info_for_title(
        self, processed_years: set[str], prev_version_years: set[str]
    ) -> dict[str, str | list]:
        new_info = {"last_modification_date": self._generation_date}

        if processed_years == prev_version_years:
            # exactly all previous years were updated, none added: modification, full title
            new_info.update(init_media_info(add=False, fields=self.patched_fields))

        elif processed_years & prev_version_years == processed_years:
            # part of the previous years were updated, no new years added: modification, yearly
            new_info.update(
                init_media_info(
                    add=False,
                    full_title=False,
                    years=list(processed_years),
                    fields=self.patched_fields,
                )
            )
        elif processed_years & prev_version_years == prev_version_years:
            # all previous years were updated, and new years added: addition, full title
            new_info.update(init_media_info(fields=self.patched_fields))
        else:
            # intersection is equal to none of the two original sets.
            # part of the previous years were updated, and new years added: addition, yearly
            new_info.update(
                init_media_info(
                    full_title=False,
                    years=list(processed_years),
                    fields=self.patched_fields,
                )
            )
        new_info["code_git_commit"] = "self.commit_url"

        return new_info

    def generate_media_dict(self, old_media_list: dict[str, dict]) -> tuple[dict, bool]:
        addition = False
        for title, yearly_stats in self._processing_stats.items():
            # if title not yet present in media list, initialize new media dict
            if title not in old_media_list:
                # new title added to the list: addition, full title
                old_media_list[title] = self.new_media(title)
            else:
                # if title was already present, update the information with current processing
                media_update_info = self.update_info_for_title(
                    set(yearly_stats.keys()),
                    set(old_media_list[title]["stats_as_dict"].keys()),
                )
                old_media_list[title].update(media_update_info)
                logger.debug("Updated media information for %s", title)

            if not addition and old_media_list[title]["update_type"] == "addition":
                # only one addition is enough
                addition = True

            for year, stats in yearly_stats.items():
                if (
                    year not in old_media_list[title]["stats_as_dict"]
                    and old_media_list[title]["updated_years"] != []
                ):
                    assert year in old_media_list[title]["updated_years"]
                print(title, year)
                old_media_list[title]["stats_as_dict"][year] = stats

        return old_media_list, addition

    def aggregate_stats_for_title(self, title: str, media_dict: dict[str, Any]):
        logger.debug("Aggregating title-level stats for %s.", title)
        # instantiate a NewspaperStatistics object for the title
        title_cumm_stats = NewspaperStatistics("canonical", "title", title)
        # instantiate the list of counts for display
        pretty_counts = []
        for _, np_year_stat in media_dict["stats_as_dict"].items():
            # newly added titles will be NewspaperStatistics objects
            if type(np_year_stat) == NewspaperStatistics:
                # add the title yearly counts
                title_cumm_stats.add_counts(np_year_stat.counts)
                pretty_counts.append(np_year_stat.pretty_print())
            else:
                # non-modified stats will be in pretty-print dict format and can be added directly
                title_cumm_stats.add_counts(np_year_stat["nps_stats"])
                pretty_counts.append(np_year_stat)
        # insert the title-level statistics at the top of the statistics
        pretty_counts.insert(0, title_cumm_stats.pretty_print())
        media_dict["media_statistics"] = pretty_counts

        return media_dict, title_cumm_stats

    def compute(self, push_to_git: bool = True) -> None:
        # function that will perform all the logic to construct the manifest
        # (similarly to NewsPaperPages)

        self._generation_date = strftime("%Y-%m-%d %H:%M:%S")

        #### IMPLEMENT LOGIC AND FILL MANIFEST DATA

        # load previous version of this manifest
        prev_version_mft = self._get_prev_version_manifest()

        if prev_version_mft is not None:
            # ensure a non-modified version remains
            old_mft = copy.deepcopy(prev_version_mft)
            old_media_list = media_list_from_mft_json(old_mft)
        else:
            # if no previous version media list, generate media list from scratch
            old_media_list = {}

        # compare current stats to previous version stats
        updated_media_list, addition = self.generate_media_dict(old_media_list)
        #   if new keys exist --> addition flag --> major increment
        #   update previous version media list with current processing media list:
        #       - setting new modification date & git url for each modified title
        #       - compute update level & targets if not patch

        logger.info("Computing the title-level statistics.")
        # generate title-level stats
        full_title_stats = []
        for title, media_as_dict in updated_media_list.items():
            # update the canonical_media_list with the new media_dict
            updated_media_list[title], title_cumm_stats = (
                self.aggregate_stats_for_title(title, media_as_dict)
            )
            # remove the stats in dict format
            del updated_media_list[title]["stats_as_dict"]
            # save the title level statistics for the overall statistics
            full_title_stats.append(title_cumm_stats)

        # generate overall stats & append input manifest overall stats
        overall_stats = None

        logger.info("Computing the overall statistics")
        # compute the overall statistics
        corpus_stats = NewspaperStatistics(self.stage, "corpus", "")
        for np_stats in full_title_stats:
            corpus_stats.add_counts(np_stats.counts)
        # add the number of titles present in corpus
        corpus_stats.add_counts({"titles": len(full_title_stats)})

        # add these overall counts to the ones of previous stages
        overall_stats = self._get_input_data_overall_stats()
        overall_stats.append(corpus_stats.pretty_print())

        # compute current version
        self.version = self._get_current_version(addition)

        # clone the data release repository locally if not for debug
        out_repo = clone_git_repo(self.temp_dir, branch=self.branch)

        # the canonical has no input stage
        if self.stage != DataStage.canonical:
            input_mft_git_path = os.path.join(
                self._get_out_path_within_repo(stage=self._input_stage),
                self.input_manifest_s3_path.split("/")[-1],
            )
        else:
            input_mft_git_path = None

        # populate the dict with all gathered information
        self.manifest_data = {
            "mft_generation_date": self._generation_date,
            "mft_s3_path": self.output_mft_s3_path,
            "input_mft_s3_path": self.input_manifest_s3_path,
            "input_mft_git_path": input_mft_git_path,
            "code_git_commit": self.commit_url,
            "media_list": list(updated_media_list.values()),
            "overall_statistics": overall_stats,
            "notes": self.notes,
        }

        success = self.validate_and_export_manifest(out_repo if push_to_git else None)

        if success:
            logger.info(
                "%s Manifest successfully generated and uploaded to S3! %s",
                "-" * 15,
                "-" * 15,
            )
