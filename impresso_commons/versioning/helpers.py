"""Helper functions to read, generate and write data versioning manifests.
"""

from ast import literal_eval
from collections import Counter
import json
import logging
import os
import re
import copy

from typing import Any, Union
from typing_extensions import Self

try:
    from enum import StrEnum
except:
    # compatibility with python 3.9
    from strenum import StrEnum
from dask import dataframe as dd
import dask.bag as db

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

    CANONICAL = "canonical"
    REBUILT = "rebuilt"
    EVENIZED = "evenized-rebuilt"
    PASSIM = "passim"
    EMBEDDINGS = "embeddings"
    ENTITIES = "entities"
    LANGIDENT = "langident"
    LINGUISTIC_PROCESSING = "lingproc"
    OCRQA = "orcqa"
    TEXT_REUSE = "text-reuse"
    TOPICS = "topics"
    SOLR_TEXT = "solr-text"
    SOLR_ENTITIES = "solr-entities"
    SOLR_EMBS = "solr-emb"

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
) -> Union[DataStage, str, None]:
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


def validate_version(
    v: str, regex: str = "^v([0-9]+[.]){2}[0-9]+$"
) -> Union[str, None]:
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


def extract_version(name_or_path: str, as_int: bool = False) -> Union[str, list[str]]:
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
    bucket_name: str, data_stage: str, partition: Union[str, None] = None
) -> Union[str, None]:

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
        "passim",
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
    return sorted(matches, key=lambda x: extract_version(x, as_int=True))[-1]


def read_manifest_from_s3(
    bucket_name: str,
    data_stage: Union[DataStage, str],
    partition: Union[str, None] = None,
) -> Union[tuple[str, dict[str, Any]], tuple[None, None]]:
    # read and extract the contents of an arbitrary manifest, to be returned in dict format.
    manifest_s3_path = find_s3_data_manifest_path(bucket_name, data_stage, partition)
    if manifest_s3_path is None:
        logger.info("No %s manifest found in bucket %s", data_stage, bucket_name)
        return None, None

    raw_text = alternative_read_text(
        manifest_s3_path, IMPRESSO_STORAGEOPT, line_by_line=False
    )

    return manifest_s3_path, json.loads(raw_text)


def read_manifest_from_s3_path(manifest_s3_path: str) -> Union[dict[str, Any], None]:
    # read and extract the contents of an arbitrary manifest, to be returned in dict format.
    try:
        raw_text = alternative_read_text(
            manifest_s3_path, IMPRESSO_STORAGEOPT, line_by_line=False
        )
        return json.loads(raw_text)
    except FileNotFoundError as e:
        logger.error("No manifest found at s3 path %s. %s", manifest_s3_path, e)
        return None


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

    # if the repository was already cloned, pull and return it.
    if os.path.exists(repo_path) and is_git_repo(repo_path):
        logger.info(
            "Git repository %s had already been cloned, pulling from branch %s.",
            repo_name,
            branch,
        )
        print(
            "Git repository %s had already been cloned, pulling from branch %s.",
            repo_name,
            branch,
        )
        repo = git.Repo(repo_path)
        # check if the current branch is the correct one & pull latest version
        if branch not in repo.active_branch.name:
            logger.info(
                "Switching branch from %s to %s", repo.active_branch.name, branch
            )
            print("Switching branch from %s to %s", repo.active_branch.name, branch)
            repo.git.checkout(branch)
        repo.remotes.origin.pull()

        return repo

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
    commit_msg: Union[str, None] = None,
) -> tuple[bool, str]:
    # given the serialized dump or a json file, write it in local git repo
    # folder and push it to the given subpath on git
    local_repo_base_dir = git_repo.working_tree_dir

    git_path = os.path.join(local_repo_base_dir, path_in_repo)
    # write file in git repo cloned locally
    full_git_path = write_dump_to_fs(file_contents, git_path, filename)

    return git_commit_push(full_git_path, git_repo, commit_msg), full_git_path


def git_commit_push(
    full_git_filepath: str, git_repo: git.Repo, commit_msg: Union[str, None] = None
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

        push_msg = f"Pushing {filename} with commit message '{commit_msg}'"
        logger.info(push_msg)
        origin.push()

        return True
    except git.exc.GitError as e:
        err_msg = f"Error while pushing {filename} to its remote repository. \n{e}"
        logger.error(err_msg)
        return False


def get_head_commit_url(repo: git.Repo) -> str:
    # final url of shape 'https://github.com/[orga_name]/[repo_name]/commit/[hash]'
    # warning --> commit on the repo's current branch!
    commit_hash = str(repo.head.commit)
    # url of shape 'git@github.com:[orga_name]/[repo_name].git'
    # or of shape 'https://github.com/[orga_name]/[repo_name].git'
    raw_url = repo.remotes.origin.url
    if raw_url.startswith("https://"):
        url_end = "/".join(["", "commit", commit_hash])
        return raw_url.replace(".git", url_end)

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
        if media["media_title"] not in ["0002088", "0002244"]:
            yearly_media_stats = {
                year_stats["element"].split("-")[1]: year_stats
                for year_stats in media["media_statistics"]
                if year_stats["granularity"] == "year"
            }

            new_media_list[media["media_title"]] = media
            new_media_list[media["media_title"]]["stats_as_dict"] = yearly_media_stats
        else:
            logger.info(
                "Skipping %s as it's BL and only a sample.", media["media_title"]
            )

    return new_media_list


def init_media_info(
    add: bool = True,
    full_title: bool = True,
    years: Union[list[str], None] = None,
    fields: Union[list[str], None] = None,
) -> dict[str, Any]:
    return {
        "update_type": "addition" if add else "modification",
        "update_level": "title" if full_title else "year",
        "updated_years": sorted(years) if years is not None else [],
        "updated_fields": fields if fields is not None else [],
    }


def counts_for_canonical_issue(
    issue: dict[str, Any], include_np_yr: bool = False
) -> dict[str, int]:

    counts = (
        {
            "np_id": issue["id"].split("-")[0],
            "year": issue["id"].split("-")[1],
        }
        if include_np_yr
        else {}
    )
    counts.update(
        {
            "issues": 1,
            "pages": len(set(issue["pp"])),
            "content_items_out": len(issue["i"]),
            "images": len([item for item in issue["i"] if item["m"]["tp"] == "image"]),
        }
    )
    return counts


def counts_for_rebuilt(
    rebuilt_ci: dict[str, Any], include_np: bool = False, passim: bool = False
) -> dict[str, Union[int, str]]:

    split_id = rebuilt_ci["id"].split("-")
    counts = {"np_id": split_id[0]} if include_np else {}
    counts.update(
        {
            "year": split_id[1],
            "issues": "-".join(split_id[:-1]),  # count the issues represented
            "content_items_out": 1,
        }
    )
    if not passim:
        counts.update(
            {
                "ft_tokens": (
                    len(rebuilt_ci["ft"].split()) if "ft" in rebuilt_ci else 0
                ),  # split on spaces to count tokens
            }
        )

    print(f"id: {rebuilt_ci['id']}")
    return counts


def compute_stats_in_canonical_bag(
    s3_canonical_issues: db.core.Bag,
) -> list[dict[str, Any]]:
    """Computes number of issues and pages per newspaper from a Dask bag of canonical data.

    Args:
        s3_canonical_issues (db.core.Bag): Bag with the contents of canonical files to
            compute statistics on.

    Returns:
        list[dict[str, Any]]: List of counts that match canonical DataStatistics keys.
    """

    print("Fetched all issues, gathering desired information.")
    logger.info("Fetched all issues, gathering desired information.")
    pages_count_df = (
        s3_canonical_issues.map(
            lambda i: counts_for_canonical_issue(i, include_np_yr=True)
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": int,
                "pages": int,
                "images": int,
                "content_items_out": int,
            }
        )
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        pages_count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": sum,
                "pages": sum,
                "content_items_out": sum,
                "images": sum,
            }
        )
        .reset_index()
    )

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")
    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


# define locally the nunique() aggregation function for dask
def chunk(s):
    # The function applied to the individual partition (map)
    return s.apply(lambda x: list(set(x)))


def agg(s):
    # The function which will aggregate the result from all the partitions (reduce)
    s = s._selected_obj
    return s.groupby(level=list(range(s.index.nlevels))).sum()


def finalize(s):
    # The optional function that will be applied to the result of the agg_tu functions
    return s.apply(lambda x: len(set(x)))


# aggregating function implementing np.nunique()
tunique = dd.Aggregation("tunique", chunk, agg, finalize)


def compute_stats_in_rebuilt_bag(
    rebuilt_articles: db.core.Bag,
    key: str = "",
    include_np: bool = False,
    passim: bool = False,
) -> list[dict[str, Union[int, str]]]:
    # key can be a title-year (include_titles=False), or lists of titles (include_titles=True)
    # when called in the rebuilt, all the rebuilt articles in the bag are from the same newspaper and year
    print("Fetched all files, gathering desired information.")
    logger.info("Fetched all files, gathering desired information.")

    # define the list of columns in the dataframe
    df_meta = {"np_id": str} if include_np else {}
    df_meta.update(
        {
            "year": str,
            "issues": str,
            "content_items_out": int,
        }
    )
    if not passim:
        df_meta.update(
            {
                "ft_tokens": int,
            }
        )

    rebuilt_count_df = (
        rebuilt_articles.map(
            lambda rf: counts_for_rebuilt(rf, include_np=include_np, passim=passim)
        )
        .to_dataframe(meta=df_meta)
        .persist()
    )

    gp_key = ["np_id", "year"] if include_np else "year"
    # agggregate them at the scale of the entire corpus
    # first groupby title, year and issue to also count the individual issues present
    if not passim:
        aggregated_df = rebuilt_count_df.groupby(by=gp_key).agg(
            {"issues": tunique, "content_items_out": sum, "ft_tokens": sum}
        )
    else:
        aggregated_df = rebuilt_count_df.groupby(by=gp_key).agg(
            {"issues": tunique, "content_items_out": sum}
        )

    # when titles are included, multiple titles and years will be represented
    if include_np:
        aggregated_df = aggregated_df.reset_index()

    msg = "Obtaining the yearly rebuilt statistics"
    if key != "":
        logger.info("%s for %s", msg, key)
    else:
        logger.info(msg)

    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_entities_bag(
    s3_entities: db.core.Bag,
) -> list[dict[str, Any]]:
    """TODO

    Args:
        s3_entities (db.core.Bag): Bag with the contents of entity files.

    Returns:
        list[dict[str, Any]]: List of counts that match NE DataStatistics keys.
    """
    pages_count_df = (
        s3_entities.map(
            lambda ci: {
                "np_id": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "ne_mentions": len(ci["nes"]),
                "ne_entities": sorted(
                    list(set([m["wkd_id"] for m in ci["nes"] if m["wkd_id"] != "NIL"]))
                ),  # sorted list to ensure all are the same
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "ne_mentions": int,
                "ne_entities": object,
            }
        )
        .explode("ne_entities")
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        pages_count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "ne_mentions": sum,
                "ne_entities": tunique,
            }
        )
        .reset_index()
    )

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")
    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_langident_bag(s3_langident: db.core.Bag) -> list[dict[str, Any]]:

    def freq(x, col="lang_fd"):
        x[col] = dict(Counter(literal_eval(x[col])))
        return x

    pages_count_df = (
        s3_langident.map(
            lambda ci: {
                "np_id": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "images": 1 if ci["tp"] == "img" else 0,
                "lang_fd": "None" if ci["lg"] is None else ci["lg"],
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "images": int,
                "lang_fd": object,
            }
        )
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        pages_count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "images": sum,
                "lang_fd": list,
            }
        )
        .reset_index()
    )
    # Dask dataframes did not support using literal_eval
    agg_bag = aggregated_df.to_bag(format="dict").map(freq)

    return agg_bag.compute()
