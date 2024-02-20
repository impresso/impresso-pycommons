"""This module contains the definition of a manifest class.

A manifest object should be instantiated for each processing step of the data
preprocessing and augmentation of the Impresso project.  
"""

import logging
import os
import json
import copy

from typing import Any
from time import strftime
from git import Repo

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
    read_manifest_from_s3_path,
)
from impresso_commons.versioning.data_statistics import (
    NewspaperStatistics,
    DataStatistics,
)
from impresso_commons.utils.s3 import get_storage_options, upload_to_s3
from impresso_commons.utils.utils import validate_against_schema

logger = logging.getLogger(__name__)

GIT_REPO_SSH_URL = "git@github.com:impresso/impresso-data-release.git"
REPO_BRANCH_URL = "https://github.com/impresso/impresso-data-release/tree/{branch}"

IMPRESSO_STORAGEOPT = get_storage_options()


class DataManifest:

    def __init__(
        self,
        data_stage: DataStage | str,
        s3_output_bucket: str,  # including partition
        git_repo: Repo,
        temp_dir: str,
        s3_input_bucket: str | None = None,  # None if canonical
        staging: bool | None = None,
        # to directly provide the next version
        new_version: str | None = None,
        # to indicate if patch in later stages
        is_patch: bool | None = False,
        # to indcate patch in canonical/rebuilt
        patched_fields: dict[str, list[str]] | list[str] | None = None,
        # directly provide the s3 path of the manifest to use as base
        previous_mft_path: str | None = None,
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
        self._prev_mft_s3_path = previous_mft_path
        # self._prev_v_mft = None
        self._prev_v_mft_yearly_stats = None
        self.prev_version = None

        # init attribute of input_manifest
        self.input_manifest_s3_path = None

        # if user already knows the target version
        self.version = None if new_version is None else validate_version(new_version)

        # if update is a patch, patched fields should be provided
        # either as list of fields or dict mapping each media to its patched fields
        self.patched_fields = patched_fields
        self.is_patch = is_patch or (patched_fields is not None)

        # example of 1 yearly stats format to gather the necessary keys
        self._eg_yearly_stats = NewspaperStatistics(self.stage, "year")

        # dict mapping from title to year to np_stat,
        # where the statistics will be aggregated during the processing
        self._processing_stats = {}
        self.manifest_data = {}
        self._generation_date = None

        logger.info("DataManifest for %s stage successfully initialized.", self.stage)

    @property
    def _input_stage(self) -> DataStage:
        return (
            DataStage.CANONICAL
            if self.stage in [DataStage.REBUILT, DataStage.CANONICAL]
            else DataStage.REBUILT
        )

    @property
    def _manifest_filename(self) -> str:
        if self.version is None:
            logger.warning("The manifest name is only available once the version is.")
            return ""

        return f"{self.stage.value}_{self.version.replace('.', '-')}.json"

    @property
    def output_mft_s3_path(self) -> str:
        if self.version is None:
            logger.warning(
                "The manifest s3 path is only available once the version is."
            )
            return ""

        if self.output_s3_partition is not None:
            s3_path = os.path.join(self.output_s3_partition, self._manifest_filename)
        else:
            s3_path = self._manifest_filename

        full_s3_path = os.path.join("s3://", self.output_bucket_name, s3_path)

        # sanity check
        if (
            self._prev_mft_s3_path is not None
            and self.output_bucket_name in self._prev_mft_s3_path
        ):
            assert (
                self._prev_mft_s3_path.split(f"/{self.stage.value}_v")[0]
                == full_s3_path.split(f"/{self.stage.value}_v")[0]
            ), "Mismatch between s3 path of previous & current version of manifest."

        return full_s3_path

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
        # previous version manifest is in the output bucket, except when:
        # _prev_mft_s3_path is defined upon instantiation, then use it directly
        logger.debug("Reading the previous version of the manifest from S3.")
        if self._prev_mft_s3_path is None:
            (self._prev_mft_s3_path, prev_v_mft) = read_manifest_from_s3(
                self.output_bucket_name, self.stage, self.output_s3_partition
            )
        else:
            prev_v_mft = read_manifest_from_s3_path(self._prev_mft_s3_path)

        if self._prev_mft_s3_path is None or prev_v_mft is None:
            logger.info(
                "No existing previous version of this manifest. Version will be v0.0.1"
            )
            return None

        self.prev_version = prev_v_mft["mft_version"]

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
        # reading the input manifest only if the input s3 bucket is defined
        if self.input_bucket_name is not None:
            logger.debug("Reading the input data's manifest from S3.")

            # only the rebuilt uses the canonical as input
            (self.input_manifest_s3_path, input_v_mft) = read_manifest_from_s3(
                self.input_bucket_name.replace("s3://", ""), self._input_stage
            )

            assert self.input_manifest_s3_path == input_v_mft["mft_s3_path"]

        # fetch the overall statistics from the input data (it's a list!)
        return (
            input_v_mft["overall_statistics"]
            if self.stage != DataStage.CANONICAL
            else []
        )

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

    def validate_and_export_manifest(
        self,
        path_to_schema: str,
        push_to_git: bool = False,
        commit_msg: str | None = None,
    ) -> bool:
        msg = "Validating and exporting manifest to s3"

        # validate the manifest against the schema
        validate_against_schema(self.manifest_data, path_to_schema)

        if push_to_git:
            # clone the data release repository locally if not for debug
            out_repo = clone_git_repo(self.temp_dir, branch=self.branch)

            logger.info("%s and GitHub!", msg)
        else:
            out_repo = None
            logger.info("%s!", msg)

        # TODO add verification against JSON schema

        manifest_dump = json.dumps(self.manifest_data, indent=4)

        mft_filename = self._manifest_filename

        # if out_repo is None, in debug mode
        if push_to_git:
            # write file and push to git
            local_path_in_repo = self._get_out_path_within_repo()
            pushed, out_file_path = write_and_push_to_git(
                manifest_dump,
                out_repo,
                local_path_in_repo,
                mft_filename,
                commit_msg=commit_msg,
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

    def has_title_year_key(self, title: str, year: str) -> bool:
        if title in self._processing_stats:
            return year in self._processing_stats[title]

        return False

    def _modify_processing_stats(
        self, title: str, year: str, counts: dict[str, int], adding: bool = True
    ) -> bool:
        # check if title/year pair is already in processing stats
        if self.has_title_year_key(title, year):
            # if title in self._processing_stats:
            #    if year in self._processing_stats[title]:
            success = self._processing_stats[title][year].add_counts(
                counts, replace=(not adding)
            )

            if not success:
                action = "adding" if adding else "replacing with"
                self._log_failed_action(title, year, action)
            # notify user of outcome
            return success

        self._processing_stats[title] = {}

        # initialize new statistics for this title-year pair:
        self._processing_stats[title][year], success = self._init_yearly_stats(
            title, year, counts
        )

        return success

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
            "last_modification_date": self._generation_date,
        }
        media.update(init_media_info(fields=self.patched_fields))
        media.update(
            {
                "code_git_commit": self.commit_url,
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
        new_info["code_git_commit"] = self.commit_url

        return new_info

    def generate_media_dict(self, old_media_list: dict[str, dict]) -> tuple[dict, bool]:
        #   if new keys exist --> addition flag --> major increment
        #   update previous version media list with current processing media list:
        #       - setting new modification date & git url for each modified title
        #       - compute update level & targets if not patch
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
                # todo, change to update??
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

    def title_level_stats(
        self, media_list: dict[str, dict]
    ) -> tuple[list[DataStatistics], dict[str, dict]]:
        full_title_stats = []
        for title, media_as_dict in media_list.items():
            # update the canonical_media_list with the new media_dict
            media_as_dict, title_cumm_stats = self.aggregate_stats_for_title(
                title, media_as_dict
            )
            # remove the stats in dict format
            del media_as_dict["stats_as_dict"]
            # save the title level statistics for the overall statistics
            full_title_stats.append(title_cumm_stats)

        return full_title_stats, media_list

    def overall_stats(self, title_stats: list[DataStatistics]) -> list[dict]:
        # generate overall stats & append input manifest overall stats

        corpus_stats = NewspaperStatistics(self.stage, "corpus", "")
        for np_stats in title_stats:
            corpus_stats.add_counts(np_stats.counts)
        # add the number of titles present in corpus
        corpus_stats.add_counts({"titles": len(title_stats)})

        # add these overall counts to the ones of previous stages
        overall_stats = self._get_input_data_overall_stats()
        overall_stats.append(corpus_stats.pretty_print())

        return overall_stats

    def compute(
        self, export_to_git_and_s3: bool = True, commit_msg: str | None = None
    ) -> None:
        # function that will perform all the logic to construct the manifest
        # (similarly to NewsPaperPages)

        if not self._processing_stats:
            msg = "The manifest cannot be computed without having provided any statistics!"
            logger.warning(msg)
            return None

        logger.info("Starting to compute the manifest...")

        self._generation_date = strftime("%Y-%m-%d %H:%M:%S")

        #### IMPLEMENT LOGIC AND FILL MANIFEST DATA

        logger.info("Loading the previous version of this manifest if it exists.")
        # load previous version of this manifest
        prev_version_mft = self._get_prev_version_manifest()

        if prev_version_mft is not None:
            # ensure a non-modified version remains
            old_mft = copy.deepcopy(prev_version_mft)
            old_media_list = media_list_from_mft_json(old_mft)
        else:
            # if no previous version media list, generate media list from scratch
            old_media_list = {}

        logger.info("Updating the media statistics with the new information...")
        # compare current stats to previous version stats
        updated_media, addition = self.generate_media_dict(old_media_list)

        logger.info("Computing the title-level statistics...")
        full_title_stats, updated_media = self.title_level_stats(updated_media)

        logger.info("Computing the overall statistics...")
        overall_stats = self.overall_stats(full_title_stats)

        # compute current version
        self.version = self._get_current_version(addition)

        # the canonical has no input stage
        if self.stage != DataStage.CANONICAL:
            input_mft_git_path = os.path.join(
                self._get_out_path_within_repo(stage=self._input_stage),
                self.input_manifest_s3_path.split("/")[-1],
            )
        else:
            input_mft_git_path = None

        # populate the dict with all gathered information
        self.manifest_data = {
            "mft_version": self.version,
            "mft_generation_date": self._generation_date,
            "mft_s3_path": self.output_mft_s3_path,
            "input_mft_s3_path": self.input_manifest_s3_path,
            "input_mft_git_path": input_mft_git_path,
            "code_git_commit": self.commit_url,
            "media_list": list(updated_media.values()),
            "overall_statistics": overall_stats,
            "notes": self.notes,
        }

        logger.info("%s Manifest successfully generated! %s", "-" * 15, "-" * 15)

        if export_to_git_and_s3:
            # If exporting directly, wil both upload to s3 and push to git.
            success = self.validate_and_export_manifest(True, commit_msg)

            if success:
                logger.info(
                    "%s Manifest successfully uploaded to S3 and GitHub! %s",
                    "-" * 15,
                    "-" * 15,
                )