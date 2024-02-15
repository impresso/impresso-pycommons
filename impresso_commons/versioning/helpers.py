"""Helper functions to read, generate and write data versioning manifests.
"""

import json
import logging
import os
import re
import copy

from typing import Any, Self
from enum import StrEnum

import git

from impresso_commons.utils.s3 import (
    fixed_s3fs_glob,
    alternative_read_text,
    get_storage_options,
    get_boto3_bucket,
)

logger = logging.getLogger(__name__)

POSSIBLE_GRANULARITIES = ["corpus", "title", "year"]
IMPRESSO_STORAGEOPT = get_storage_options()

VERSION_INCREMENTS = ["major", "minor", "patch"]


class DataStage(StrEnum):
    """Enum all stages requiring a versioning manifest.

    Each member corresponds to a data stage and the associated string is used to name
    each generated manifest accordingly.

    TODO: finalize the exact list of names and strings based on needs.
    TODO: add options for data indexing in Solr
    """

    canonical = "canonical"
    rebuilt = "rebuilt"
    evenized = "evenized-rebuilt"
    embeddings = "embeddings"  # todo change to have each type of embedding represented
    entities = "entities"
    langident = "langident"
    linguistic_processing = "lingproc"
    mentions = "mentions"
    orcqa = "orcqa"
    text_reuse = "text-reuse"
    topics = "topics"

    @classmethod
    def has_value(cls: Self, value: str) -> bool:
        """Check if enum contains given value

        Args:
            cls (Self): This DataStage class
            value (str): Value to check

        Returns:
            bool: True if the value provided is in this enum's values, False otherwise.
        """
        return value in cls._value2member_map_


##################################
###### VALIDATION FUNCTIONS ######


def validate_stage(
    data_stage: str, return_value_str: bool = False
) -> DataStage | str | None:
    """Validate the provided data stage if it's in the DataStage Enum (key or value).

    Args:
        data_stage (str): Data stage key or value to validate.
        return_value_str (bool, optional): Whether to return the data stage's value if
            it was valid. Defaults to False.

    Raises:
        e: The provided str is neither a data stage key nor value.

    Returns:
        DataStage | str | None: The corresponding DataStage or value string if valid.
    """
    try:
        if DataStage.has_value(data_stage):
            stage = DataStage(data_stage)
        else:
            stage = DataStage[data_stage]
        return stage.value if return_value_str else stage
    except ValueError as e:
        err_msg = f"{e} \nProvided data stage '{data_stage}' is not a valid data stage."
        logger.critical(err_msg)
        raise e


def validate_granularity(value: str, for_stats: bool = True):
    lower = value.lower()
    if lower in POSSIBLE_GRANULARITIES:
        if not for_stats or (for_stats and lower != "issue"):
            return lower
    # only incorrect granularity values will not be returned
    logger.critical("Provided granularity '%s' isn't valid.", lower)
    raise ValueError


###############################
###### VERSION FUNCTIONS ######


def validate_version(v: str, regex: str = "^v([0-9]+[.]){2}[0-9]+$") -> str | None:
    # accept versions with hyphens in case of mistake
    v = v.replace("-", ".")

    if re.match(regex, v) is not None:
        return v

    msg = f"Non conforming version {v} provided: ({regex}), version will be inferred."
    logger.critical(msg)
    return None


def version_as_list(version: str) -> list[int]:
    start = 1 if version[0] == "v" else 0
    sep = "." if "." in version else "-"
    return version[start:].split(sep)


def extract_version(name_or_path: str, as_int: bool = False) -> str | list[str]:
    # in the case it's a path
    basename = os.path.basename(name_or_path)
    version = basename.replace(".json", "").split("_")[-1]

    return int(version[1:].replace("-", "")) if as_int else version.replace("-", ".")


def increment_version(prev_version: str, increment: str) -> str:

    try:
        incr_val = VERSION_INCREMENTS.index(increment)
        list_v = version_as_list(prev_version)
        # increase the value of the correct "sub-version" and reset the ones right of it
        list_v[incr_val] = str(int(list_v[incr_val]) + 1)
        if incr_val < 2:
            list_v[incr_val + 1 :] = ["0"] * (2 - incr_val)
        return "v" + ".".join(list_v)
    except ValueError as e:
        logger.error(
            "Provided invalid increment %s: not in %s", increment, VERSION_INCREMENTS
        )
        raise e


#####################################
###### S3 READ/WRITE FUNCTIONS ######


def find_s3_data_manifest_path(
    bucket_name: str, data_stage: str, partition: str | None = None
) -> str | None:

    # fetch the data stage as the naming value
    if type(data_stage) == DataStage:
        stage_value = data_stage.value
    else:
        stage_value = validate_stage(data_stage, return_value_str=True)

    # manifests have a json extension and are named after the format (value)
    path_filter = f"{stage_value}_v*.json"

    if partition is None and stage_value in [
        "canonical",
        "rebuilt",
        "evenized-rebuilt",
    ]:
        # manifest in top-level partition of bucket
        bucket = get_boto3_bucket(bucket_name)
        matches = fixed_s3fs_glob(path_filter, boto3_bucket=bucket)
    else:
        assert partition is not None, "partition should be provided for processed data"
        # processed data are all in the same bucket,
        # manifest should be directly fetched from path
        full_s3_path = os.path.join(bucket_name, partition, path_filter)
        print(full_s3_path)
        matches = fixed_s3fs_glob(full_s3_path)

    # matches will always be a list
    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0:
        # no matches means it's hte first manifest for the stage or bucket
        return None
    # if multiple versions exist, return the latest one
    return sorted(matches, lambda x: extract_version(x, as_int=True))[-1]


def read_manifest_from_s3(
    bucket_name: str, data_stage: DataStage | str, partition: str | None = None
) -> tuple[str, dict[str, Any]] | tuple[None, None]:
    # read and extract the contents of an arbitrary manifest, to be returned in dict format.
    manifest_s3_path = find_s3_data_manifest_path(bucket_name, data_stage, partition)

    if manifest_s3_path is None:
        logger.info("No %s manifest found in bucket %s", data_stage, bucket_name)
        return None, None

    raw_text = alternative_read_text(
        manifest_s3_path, IMPRESSO_STORAGEOPT, line_by_line=False
    )

    return manifest_s3_path, json.loads(raw_text)


def list_and_date_s3_files():
    # return a list of (file, date) pairs from a given s3 bucket,
    # to extract their last modification date if no previous manifest exists.
    pass


###########################
###### GIT FUNCTIONS ######


def write_dump_to_fs(file_contents: str, abs_path: str, filename: str) -> str:
    full_file_path = os.path.join(abs_path, filename)

    # write file to absolute path provided
    with open(full_file_path, "w", encoding="utf-8") as outfile:
        outfile.write(file_contents)

    return full_file_path


def is_git_repo(path: str) -> bool:
    """Check if a directory contains a Git repository.

    Args:
        path (str): The path to the directory to be checked.

    Returns:
        bool: True if the directory contains a Git repository, False otherwise.
    """
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False
    except git.NoSuchPathError:
        return False


def clone_git_repo(
    path: str, repo_name: str = "impresso/impresso-data-release", branch: str = "master"
) -> git.Repo:
    # WARNING: path should be absolute path!!!
    repo_ssh_url = f"git@github.com:{repo_name}.git"
    repo_https_url = f"https://github.com/{repo_name}.git"

    repo_path = os.path.join(path, repo_name.split("/")[1])

    # if the repository was already cloned, return it.
    if os.path.exists(repo_path) and is_git_repo(repo_path):
        logger.debug("Git repository %s had already been cloned.", repo_name)
        return git.Repo(repo_path)

    # try to clone using ssh, if it fails, retry with https.
    try:
        logger.info("Cloning the %s git repository with ssh.", repo_name)
        return git.Repo.clone_from(repo_ssh_url, repo_path, branch=branch)
    except git.exc.GitCommandError as e:
        err_msg = (
            f"Error while cloning the git repository {repo_name} using ssh, trying "
            f"with https. \n{e}"
        )
        logger.warning(err_msg)
    # Fallback to https
    try:
        logger.info("Cloning the %s git repository with https.", repo_name)
        return git.Repo.clone_from(repo_https_url, repo_path, branch=branch)
    except Exception as e:
        err_msg = (
            f"Error while cloning the git repository {repo_name}, it was not possible "
            f"to clone it with ssh or https. \n{e}"
        )
        logger.critical(err_msg)
        raise e


def write_and_push_to_git(
    file_contents: str,
    git_repo: git.Repo,
    path_in_repo: str,
    filename: str,
    commit_msg: str | None = None,
) -> tuple[bool, str]:
    # given the serialized dump or a json file, write it in local git repo
    # folder and push it to the given subpath on git
    local_repo_base_dir = git_repo.working_tree_dir

    git_path = os.path.join(local_repo_base_dir, path_in_repo)
    # write file in git repo cloned locally
    full_git_path = write_dump_to_fs(file_contents, git_path, filename)

    return git_commit_push(full_git_path, git_repo, commit_msg), full_git_path


def git_commit_push(
    full_git_filepath: str, git_repo: git.Repo, commit_msg: str | None = None
) -> bool:
    # add, commit and push the file at the given path.
    filename = os.path.basename(full_git_filepath)
    # git add file
    git_repo.index.add([full_git_filepath])
    try:
        # git commit and push
        if commit_msg is None:
            commit_msg = f"Add generated manifest file {filename}."
        git_repo.index.commit(commit_msg)
        origin = git_repo.remote(name="origin")

        push_msg = f"Pushing {filename} with commit message {commit_msg}."
        logger.info(push_msg)
        origin.push()

        return True
    except Exception as e:
        err_msg = f"Error while pushing {filename} to its remote repository. \n{e}"
        logger.error(err_msg)
        return False


def get_head_commit_url(repo: git.Repo) -> str:
    commit_hash = str(repo.head.commit)
    # url of shape 'git@github.com:[orga_name]/[repo_name].git'
    raw_url = repo.remotes.origin.url
    # now list with contents ['git', 'github', 'com', [orga_name]/[repo_name], 'git']
    url_pieces = re.split(r"[@.:]", raw_url)
    # replace the start and end of the list
    url_pieces[0] = "https:/"
    url_pieces[-1] = "commit"
    # merge back the domain name and remove excedentary element
    url_pieces[1] = ".".join(url_pieces[1:3])
    del url_pieces[2]
    # add the commit hash at the end
    url_pieces.append(commit_hash)

    return "/".join(url_pieces)


##########################################
###### MEDIA LIST & STATS FUNCTIONS ######


def media_list_from_mft_json(json_mft: dict[str, Any]) -> dict[str, dict]:
    # extract a media list (np_title -> "media" dict) from a manifest json
    # where  "media" dict also contains stats organized as a dict with years as keys.
    # prevent modification or original manifest, to recover later
    manifest = copy.deepcopy(json_mft)
    new_media_list = {}
    for media in manifest["media_list"]:
        yearly_media_stats = {
            year_stats["element"].split("-")[1]: year_stats
            for year_stats in media["media_statistics"]
            if year_stats["granularity"] == "year"
        }

        new_media_list[media["media_title"]] = media
        new_media_list[media["media_title"]]["stats_as_dict"] = yearly_media_stats

    return new_media_list


def init_media_info(
    add: bool = True,
    full_title: bool = True,
    years: list[str] | None = None,
    fields: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "update_type": "addition" if add else "modification",
        "update_level": "title" if full_title else "year",
        "updated_years": sorted(years) if years is not None else [],
        "updated_fields": fields if fields is not None else [],
    }


def yearly_stats_keys_from_mft(prev_mft: dict[str, Any]) -> dict[str, dict]:
    # extract list of title-year pairs present in the statistics of a manifest dict.
    pass
