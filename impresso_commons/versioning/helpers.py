"""Helper functions to read, generate and write data versioning manifests.
"""

import copy
import json
import logging
import os
import re
from ast import literal_eval
from collections import Counter
from time import strptime
from typing import Any, Union, Self, Optional

# python 3.10+
from enum import StrEnum
from tqdm import tqdm

import git
from dask import dataframe as dd
import dask.bag as db
from dask.distributed import progress, Client

from impresso_commons.utils.utils import bytes_to
from impresso_commons.utils.s3 import (
    fixed_s3fs_glob,
    alternative_read_text,
    get_storage_options,
    get_boto3_bucket,
    get_s3_object_size,
)

logger = logging.getLogger(__name__)


IMPRESSO_STORAGEOPT = get_storage_options()
POSSIBLE_GRANULARITIES = ["corpus", "title", "year"]
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
    OCRQA = "ocrqa"
    TEXT_REUSE = "text-reuse"
    TOPICS = "topics"
    SOLR_TEXT = "solr-ingestion-text"
    SOLR_ENTITIES = "solr-ingestion-entities"
    SOLR_EMBS = "solr-ingestion-emb"
    MYSQL_CIS = "mysql-ingestion"

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


def validate_granularity(value: str) -> Optional[str]:
    """Validate that the granularity value provided is valid.

    Statistics are computed on three granularity levels:
    corpus, title and year.

    Args:
        value (str): Granularity value to validate

    Raises:
        ValueError: The provided granularity isn't one of corpus, title and year.

    Returns:
        Optional[str]: The provided value, in lower case, or None if not valid.
    """
    lower = value.lower()
    if lower in POSSIBLE_GRANULARITIES:
        return lower
    # only incorrect granularity values will not be returned
    logger.critical("Provided granularity '%s' isn't valid.", lower)
    raise ValueError


###############################
###### VERSION FUNCTIONS ######


def validate_version(v: str, regex: str = "^v([0-9]+[.]){2}[0-9]+$") -> Optional[str]:
    """Validate the provided string version against a regex.

    The provided version should be in format "vM.m.p", where M, m and p are
    integers representing respectively the Major, minor and patch version.

    Args:
        v (str): version in string format to validate.
        regex (str, optional): Regex against which to match the version.
            Defaults to "^v([0-9]+[.]){2}[0-9]+$".

    Returns:
        Optional[str]: The provided version if it's valid, None otherwise.
    """
    # accept versions with hyphens in case of mistake
    v = v.replace("-", ".")

    if re.match(regex, v) is not None:
        return v

    msg = f"Non conforming version {v} provided: ({regex}), version will be inferred."
    logger.critical(msg)
    return None


def version_as_list(version: str) -> list[int]:
    """Return the provided string version as a list of three ints.

    Args:
        version (str): String version to return as list

    Returns:
        list[int]: list of len 3 where indices respecively correspond to the
            Major, minor and patch versions.
    """
    if version[0] == "v":
        version = validate_version(version)
        start = 1
    else:
        start = 0
    sep = "." if "." in version else "-"
    return version[start:].split(sep)


def extract_version(name_or_path: str, as_int: bool = False) -> Union[str, int]:
    """Extract the version from a string filename or path.

    This function is in particular mean to extract the version from paths or filenames
    of manifests: structured as [data-stage]_vM-m-p.json.

    Args:
        name_or_path (str): Filename or path from which to extract the version.
        as_int (bool, optional): Whether to return the extracted version as int or str.
            Defaults to False.

    Returns:
        Union[str, int]: Extracted version, as int or str based on `as_int`.
    """
    # in the case it's a path
    basename = os.path.basename(name_or_path)
    version = basename.replace(".json", "").split("_")[-1]

    return int(version[1:].replace("-", "")) if as_int else version.replace("-", ".")


def increment_version(prev_version: str, increment: str) -> str:
    """Update  given version accoding to the given increment.

    When the increment is major or minor, all following numbers are reset to 0.

    Args:
        prev_version (str): Version to increment
        increment (str): Increment, can be one of major, minor and patch.

    Raises:
        e: Increment value provided is not valid.

    Returns:
        str: Vesion incremented accordingly.
    """
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
    bucket_name: str, data_stage: str, partition: Optional[str] = None
) -> Optional[str]:
    """Find and return the latest data manifest in a given S3 bucket.

    On S3, different Data stages will be stored in different ways.
    In particular, data stages corresponding to enrichments are all placed in the
    same bucket but in different partitions.
    Data stages "canonical", "rebuilt", "evenized-rebuilt" & ones related to Solr
    are the ones where each stage has its own bucket.

    Args:
        bucket_name (str): Name of the bucket in which to look.
        data_stage (str): Data stage corresponding to the manifest to fetch.
        partition (Optional[str], optional): Partition within the bucket to look
            into. Defaults to None.

    Returns:
        Optional[str]: S3 path of the latest manifest in the bucket, None if no
            manifests were found inside.
    """
    # fetch the data stage as the naming value
    if isinstance(data_stage, DataStage):
        stage_value = data_stage.value
    else:
        stage_value = validate_stage(data_stage, return_value_str=True)

    # manifests have a json extension and are named after the format (value)
    path_filter = f"{stage_value}_v*.json"

    if partition is None and stage_value in [
        DataStage.CANONICAL.value,  # "canonical"
        DataStage.REBUILT.value,  # "rebuilt"
        DataStage.EVENIZED.value,  # "evenized-rebuilt"
        DataStage.PASSIM.value,  # "passim"
        DataStage.SOLR_TEXT.value,  # "solr-ingestion-text"
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
        # no matches means it's the first manifest for the stage or bucket
        return None

    # if multiple versions exist, return the latest one
    return sorted(matches, key=lambda x: extract_version(x, as_int=True))[-1]


def read_manifest_from_s3(
    bucket_name: str,
    data_stage: Union[DataStage, str],
    partition: Optional[str] = None,
) -> Optional[tuple[str, dict[str, Any]]]:
    """Read and load manifest given an S3 bucket.

    Args:
        bucket_name (str): NAme of the s3 bucket to look into
        data_stage (Union[DataStage, str]): Data stage corresponding to the
            manifest to fetch.
        partition (Optional[str], optional): Partition within the bucket to look
            into. Defaults to None.

    Returns:
        tuple[str, dict[str, Any]] | tuple[None, None]: S3 path of the manifest
            and corresponding contents, if a manifest was found, None otherwise.
    """
    manifest_s3_path = find_s3_data_manifest_path(bucket_name, data_stage, partition)
    if manifest_s3_path is None:
        logger.info("No %s manifest found in bucket %s", data_stage, bucket_name)
        return None, None

    raw_text = alternative_read_text(
        manifest_s3_path, IMPRESSO_STORAGEOPT, line_by_line=False
    )

    return manifest_s3_path, json.loads(raw_text)


def read_manifest_from_s3_path(manifest_s3_path: str) -> Optional[dict[str, Any]]:
    """read and extract the contents of an arbitrary manifest,

    Args:
        manifest_s3_path (str): S3 path of the manifest to read.

    Returns:
        Optional[dict[str, Any]]: Contents of manifest if found on S3, None otherwise.
    """
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


def write_dump_to_fs(file_contents: str, abs_path: str, filename: str) -> Optional[str]:
    """Write a provided string dump to the local filesystem given its path and filename.

    TODO: Potentially moving this method to `utils.py`.

    Args:
        file_contents (str): Dumped contents in str format, ready to be written.
        abs_path (str): Local path to the directory in which the file will be.
        filename (str): Filename of the file to write, including its extension.

    Returns:
        Optional[str]: Full path of writen file, or None if an IOError occurred.
    """
    full_file_path = os.path.join(abs_path, filename)

    # write file to absolute path provided
    try:
        with open(full_file_path, "w", encoding="utf-8") as outfile:
            outfile.write(file_contents)

        return full_file_path
    except IOError as e:
        logger.error(
            "Writing dump %s to local fs triggered the error: %s", full_file_path, e
        )
        return None


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
    """Clone a git repository into a given path in the local file-system.

    Args:
        path (str): Path (ideally absolute) to the dir in which to clone the git repo.
        repo_name (str, optional): Full name of the git repository to clone, as it
            appears in its URL. Defaults to "impresso/impresso-data-release".
        branch (str, optional): Specific branch to clone. Defaults to "master".

    Raises:
        e: Cloning the repo failed, both using SSH and HTTPS.

    Returns:
        git.Repo: Object representing the cloned repository if it was cloned.
    """
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
    commit_msg: Optional[str] = None,
) -> tuple[bool, str]:
    """Given a serialized dump, write it in local git repo, commit and push.

    Args:
        file_contents (str): Serialized dump of a JSON file.
        git_repo (git.Repo): Object representing the git repository to push to.
        path_in_repo (str): Relative path where to write the file.
        filename (str): Desired name for the file, including extension.
        commit_msg (Optional[str], optional): Commit message. If not defined, a
            basic message on the added manifest will be used.Defaults to None.

    Returns:
        tuple[bool, str]: Whether the process was successful and corresponding filepath.
    """
    # given the serialized dump or a json file, write it in local git repo
    # folder and push it to the given subpath on git
    local_repo_base_dir = git_repo.working_tree_dir

    git_path = os.path.join(local_repo_base_dir, path_in_repo)
    # write file in git repo cloned locally
    full_git_path = write_dump_to_fs(file_contents, git_path, filename)

    if full_git_path is not None:
        return git_commit_push(full_git_path, git_repo, commit_msg), full_git_path
    else:
        return False, os.path.join(git_path, filename)


def git_commit_push(
    full_git_filepath: str, git_repo: git.Repo, commit_msg: Optional[str] = None
) -> bool:
    """Commit and push the addition of a given file within the repository.

    TODO: make more general for non-manifest related uses?

    Args:
        full_git_filepath (str): Path to the file added to the git repository.
        git_repo (git.Repo): git.Repo object of the repository to commit and push to.
        commit_msg (Optional[str], optional): Message to use when commiting. If not
            defined, a basic message on the added manifest will be used. Defaults to None.

    Returns:
        bool: Whether the commit and push operations were successful.
    """
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


def get_head_commit_url(repo: str | git.Repo) -> str:
    """Get the URL of the last commit on a given Git repository.

    TODO: test the function when repo is https url of repository.
    TODO: provide branch argument.

    `repo` can be one of three things:
        - a git.Repo instantiated object (if alreaday instantiated outside).
        - the local path to the git repository (previously cloned).
        - the HTTPS URL to the Git repository.

    Note:
        The returned commit URL corresponds to the one on the repository's active
        branch (master for the URL).

    Args:
        repo (str | git.Repo): local path, git.Repo object or URL of the repository.

    Returns:
        str: The HTTPS URL of the last commit on the git repository's master branch.
    """
    not_repo_url = True
    if isinstance(repo, str):
        if "https" in repo:
            # if it's the https link to the repo, get the hash of last commit
            not_repo_url = False
            commit_hash = git.cmd.Git().ls_remote(repo, heads=True).split()[0]
            raw_url = repo
        else:
            # get the actual repo if it's only the path.
            repo = git.Repo(repo)

    if not_repo_url:
        # final url of shape 'https://github.com/[orga_name]/[repo_name]/commit/[hash]'
        # warning --> commit on the repo's current branch!
        commit_hash = str(repo.head.commit)
        # url of shape 'git@github.com:[orga_name]/[repo_name].git'
        # or of shape 'https://github.com/[orga_name]/[repo_name].git'
        raw_url = repo.remotes.origin.url

    if raw_url.startswith("https://"):
        if ".git" in raw_url:
            url_end = "/".join(["", "commit", commit_hash])
            return raw_url.replace(".git", url_end)
        return "/".join([raw_url, "commit", commit_hash])

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
    """Extract the `media_list` from a manifest as a dict where each title is a key.

    For each title, all fields from the original media list will still be present
    along with an additional `stats_as_dict` field containing a dict mapping each
    year to its specific statistics.

    As a result:
        - All represented titles are within the keys of the returned media list.
        - For each title, represented years are in the keys of its `stats_as_dict` field.

    Args:
        json_mft (dict[str, Any]): Dict following the JSON schema of a manifest from
            which to extract the media list.

    Returns:
        dict[str, dict]: Media list of given manifest, with `stats_as_dict` field.
    """
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
    years: Optional[list[str]] = None,
    fields: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Initialize the media update dict for a title given relevant information.

    All the update informations are relating to the newly processed data, in
    comparison with the one computed during the last processing.

    Args:
        add (bool, optional): Whether new data was added. Defaults to True.
        full_title (bool, optional): Whether all the title's years were modified.
            Defaults to True.
        years (Optional[list[str]], optional): When `full_title`, the specific years
            which were modified/updated. Defaults to None.
        fields (Optional[list[str]], optional): List of specific fields that were
            modified/updated. Defaults to None.

    Returns:
        dict[str, Any]: Instantiated dict with the update information for a given media.
    """
    return {
        "update_type": "addition" if add else "modification",
        "update_level": "title" if full_title else "year",
        "updated_years": sorted(years) if years is not None else [],
        "updated_fields": fields if fields is not None else [],
    }


def counts_for_canonical_issue(
    issue: dict[str, Any], include_np_yr: bool = False
) -> dict[str, int]:
    """Given the canonical representation of an issue, get its counts.

    Args:
        issue (dict[str, Any]): Canonical JSON representation of an issue.
        include_np_yr (bool, optional): Whether the newspaper title and year should
            be included in the returned dict for later aggregation. Defaults to False.

    Returns:
        dict[str, int]: Dict listing the counts for this issue, ready to be aggregated.
    """
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
    """Define the counts for 1 given rebuilt content-item to match the count keys.

    Args:
        rebuilt_ci (dict[str, Any]): Rebuilt content-item from which to extract counts.
        include_np (bool, optional): Whether to include the title in resulting dict,
            not necessary for on-the-fly computation. Defaults to False.
        passim (bool, optional): True if rebuilt is in passim format. Defaults to False.

    Returns:
        dict[str, Union[int, str]]: Dict with rebuilt (passim) keys and counts for 1 CI.
    """
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

    return counts


def compute_stats_in_canonical_bag(
    s3_canonical_issues: db.core.Bag, client: Client | None = None
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
    count_df = (
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
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": sum,
                "pages": sum,
                "content_items_out": sum,
                "images": sum,
            }
        )
        .reset_index()
    ).persist()

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")
    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


### DEFINITION of tunique ###


# define locally the nunique() aggregation function for dask
def chunk(s):
    """The function applied to the individual partition (map).
    Part of the ggregating function(s) implementing np.nunique()
    """
    return s.apply(lambda x: list(set(x)))


def agg(s):
    """The function which will aggregate the result from all the partitions (reduce).
    Part of the ggregating function(s) implementing np.nunique()
    """
    s = s._selected_obj
    return s.groupby(level=list(range(s.index.nlevels))).sum()


def finalize(s):
    """The optional function that will be applied to the result of the agg_tu functions.
    Part of the ggregating function(s) implementing np.nunique()
    """
    return s.apply(lambda x: len(set(x)))


# aggregating function implementing np.nunique()
tunique = dd.Aggregation("tunique", chunk, agg, finalize)

### DEFINITION of tunique ###


def compute_stats_in_rebuilt_bag(
    rebuilt_articles: db.core.Bag,
    key: str = "",
    include_np: bool = False,
    passim: bool = False,
    client: Client | None = None,
) -> list[dict[str, Union[int, str]]]:
    """Compute stats on a dask bag of rebuilt output content-items.

    Args:
        rebuilt_articles (db.core.Bag): Bag with the contents of rebuilt files.
        key (str, optional): Optionally title-year pair for on-the-fly computation.
            Defaults to "".
        include_np (bool, optional): Whether to include the title in the groupby,
            not necessary for on-the-fly computation. Defaults to False.
        passim (bool, optional): True if rebuilt is in passim format. Defaults to False.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match rebuilt or paassim
            DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
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
        aggregated_df = aggregated_df.reset_index().persist()

    msg = "Obtaining the yearly rebuilt statistics"
    if key != "":
        logger.info("%s for %s", msg, key)
    else:
        logger.info(msg)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_entities_bag(
    s3_entities: db.core.Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of entities output content-items.

    Args:
        s3_entities (db.core.Bag): Bag with the contents of entity files.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match NE DataStatistics keys.
    """
    count_df = (
        s3_entities.map(
            lambda ci: {
                "np_id": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "ne_mentions": len(ci["nes"]),
                "ne_entities": sorted(
                    list(
                        set(
                            [
                                m["wkd_id"]
                                for m in ci["nes"]
                                if "wkd_id" in m and m["wkd_id"] not in ["NIL", None]
                            ]
                        )
                    )
                ),  # sorted list to ensure all are the same
            }
        ).to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "ne_mentions": int,
                "ne_entities": object,
            }
        )
        # .explode("ne_entities")
        # .persist()
    )

    count_df["ne_entities"] = count_df["ne_entities"].apply(
        lambda x: x if isinstance(x, list) else [x]
    )
    count_df = count_df.explode("ne_entities").persist()

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "ne_mentions": sum,
                "ne_entities": tunique,
            }
        )
        .reset_index()
    ).persist()

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_langident_bag(
    s3_langident: db.core.Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of langident output content-items.

    Args:
        s3_langident (db.core.Bag): Bag of lang-id content-items.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]:  List of counts that match langident DataStatistics keys.
    """

    def freq(x, col="lang_fd"):
        x[col] = dict(Counter(literal_eval(x[col])))
        return x

    count_df = (
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
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "images": sum,
                "lang_fd": list,
            }
        )
        .reset_index()
    ).persist()

    # Dask dataframes did not support using literal_eval
    agg_bag = aggregated_df.to_bag(format="dict").map(freq)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(agg_bag)

    return agg_bag.compute()


def compute_stats_in_solr_text_bag(
    s3_solr_text: db.core.Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of content-items formatted for Solr input.

    Args:
        s3_solr_text (db.core.Bag): Bag or Solr formatted content-items.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]:  List of counts that match solr text DataStatistics keys.
    """
    count_df = (
        s3_solr_text.map(
            lambda ci: {
                "np_id": ci["meta_journal_s"],
                "year": ci["meta_year_i"],
                "issues": ci["meta_issue_id_s"],
                "content_items_out": 1,
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
            }
        )
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
            }
        )
        .reset_index()
    ).persist()

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


################ MANIFEST DIFFS #################


def manifest_summary(mnf_json: dict[str, Any], extended_summary: bool = False) -> None:
    """
    Generate a summary of the manifest data.

    Args:
        mnf_json (dict): A dictionary containing manifest data.
        extended_summary (bool, optional): Whether to include extended summary
        with year statistics. Defaults to False.

    Returns:
        None

    Prints: Summary of the manifest including the number of media items, additions,
    and modifications.

    Example:
        >>> manifest_summary(manifest_json)
        Summary of manifest /path/to/manifest.json:
        Number of media items: 10 (8 from set)
        Number of addition at title level: 5
        Number of addition at year level: 3
        Number of modification at title level: 2
        Number of modification at year level: 1
    """
    nb_media_items = len(mnf_json["media_list"])
    nb_addition_title_level = 0
    nb_addition_year_level = 0
    nb_modification_title_level = 0
    nb_modification_year_level = 0

    media_item_set = set()
    title_nbyear = {}

    for media_item in mnf_json["media_list"]:
        media_item_set.add(media_item["media_title"])
        if media_item["update_type"] == "addition":
            if media_item["update_level"] == "title":
                nb_addition_title_level += 1
            elif media_item["update_level"] == "year":
                nb_addition_year_level += 1
        elif media_item["update_type"] == "modification":
            if media_item["update_level"] == "title":
                nb_modification_title_level += 1
            elif media_item["update_level"] == "year":
                nb_modification_year_level += 1
        if extended_summary:
            title_nbyear[media_item["media_title"]] = len(
                [
                    year_stats
                    for year_stats in media_item["media_statistics"]
                    if year_stats["granularity"] == "year"
                ]
            )

    # print summary
    print(
        f"\n*** Summary of manifest: [{mnf_json['mft_s3_path']}] (regardless of any "
        f"modification date):"
    )
    print(f"- Number of media items: {nb_media_items} ({len(media_item_set)} from set)")
    print(f"- Number of addition at title level: {nb_addition_title_level}")
    print(f"- Number of addition at year level: {nb_addition_year_level}")
    print(f"- Number of modification at title level: {nb_modification_title_level}")
    print(f"- Number of modification at year level: {nb_modification_year_level}")
    print(f"- List of media titles:\n{get_media_titles(mnf_json['media_list'])}\n")

    if extended_summary:
        print(
            f"\nExtended summary - Number of years per title "
            f"(regardless of modification/addition or not):"
            f"{nb_modification_year_level}"
        )
        for key, val in title_nbyear.items():
            print(f"- {key:<18}: {val:>5}y")
            print("\n")


def filter_new_or_modified_media(
    rebuilt_mft_json: dict[str, Any], previous_mft_json: dict[str, Any]
) -> dict[str, Any]:
    """
    Compares two manifests to determine new or modified media items.

    Typical use-case is during an atomic update, when only media items added or modified
    compared to the previous process need to be ingested or processed.

    Args:
        rebuilt_mft_json (dict[str, Any]): json of the rebuilt manifest (new).
        previous_mft_json (dict[str, Any]): json of the previous process manifest.

    Returns:
        list[dict[str, Any]]: A manifest identical to 'rebuilt_mft_path' but only with
        media items that are new or modified in the media list.

    Example: >>> new_or_modified = get_new_or_modified_media("new_manifest.json",
    "previous_manifest.json") >>> print(new_or_modified) [{'media_title':
    'new_media_item_1', 'last_modif_date': '2024-04-04T12:00:00Z', etc.},
    {'media_title': 'modified_media_item_2', 'last_modif_date':
    '2024-04-03T12:00:00Z', etc.}]
    """
    filtered_manifest = copy.deepcopy(rebuilt_mft_json)

    # Extract last modification date of each media item of the previous process
    previous_media_items = {
        media["media_title"]: strptime(
            media["last_modification_date"], "%Y-%m-%d %H:%M:%S"
        )
        for media in previous_mft_json["media_list"]
    }

    # Print rebuilt manifest summary
    manifest_summary(rebuilt_mft_json, extended_summary=False)

    # Filter: keep only media items newly added or modified after last process
    filtered_media_list = []
    for rebuilt_media_item in rebuilt_mft_json["media_list"]:
        if rebuilt_media_item["media_title"] not in previous_media_items:
            filtered_media_list.append(rebuilt_media_item)
        elif (
            strptime(rebuilt_media_item["last_modification_date"], "%Y-%m-%d %H:%M:%S")
            > previous_media_items[rebuilt_media_item["media_title"]]
        ):
            filtered_media_list.append(rebuilt_media_item)

    logger.info(
        "\n*** Getting new or modified items:"
        "\nInput (rebuilt) manifest has %s media items.",
        len(get_media_titles(rebuilt_mft_json)),
    )
    logger.info(
        "Resulting filtered manifest has %s media items.", len(filtered_media_list)
    )
    logger.info(
        "Media items that will be newly processed:\n %s\n",
        get_media_titles(filtered_media_list),
    )

    filtered_manifest["media_list"] = filtered_media_list

    return filtered_manifest


def get_media_titles(
    input_data: Union[dict[str, Any], list[dict[str, Any]]]
) -> list[str]:
    """
    Extracts media titles from the input data which can be either a manifest
    or a media list.

    Args:
        input_data (Union[dict[str, Any], list[dict[str, Any]]]): A manifest dictionary
            or the media list of a manifest.

    Returns:
        list[str]: A list of media titles extracted from the input data.
        Ex:  ['Title 1', 'Title 2']
    Raises:
        TypeError: If the input data is not in the expected format.
        KeyError: If the 'media_title' key is not found in the input data.
    """
    if isinstance(input_data, list):
        titles = [media_item["media_title"] for media_item in input_data]
    else:
        titles = [media_item["media_title"] for media_item in input_data["media_list"]]
    return titles


def get_media_item_years(mnf_json: dict[str, Any]) -> dict[str, dict[str, float]]:
    """
    Retrieves the s3 key and size in MB of each year of media items from a manifest.

    Args:
        mnf_json (dict): A manifest dictionary.

    Returns:
       media_items_years (dict): A dictionary where media titles are keys,
            and each value is a dictionary with s3 key as key and its size as value.
    """

    bucket_name = mnf_json["mft_s3_path"].rsplit("/", 1)[0]
    media_items_years = {}

    logger.info("*** Retrieving size info for each key")
    for media_item in tqdm(mnf_json["media_list"]):
        title = media_item["media_title"]
        years = {}

        for media_year in media_item["media_statistics"]:
            if media_year["granularity"] != "year":
                continue

            year_key = title + "/" + media_year["element"] + ".jsonl.bz2"
            s3_key = bucket_name + "/" + year_key
            year_size_b = get_s3_object_size(bucket_name.split("//")[1], year_key)
            year_size_m = (
                round(bytes_to(year_size_b, "m"), 2)
                if year_size_b is not None
                else None
            )
            # print(f"Size in b: {year_size_b}, in mb: {year_size_m}")

            years[s3_key] = year_size_m

        media_items_years[title] = years

    logger.info("*** About the collection if s3 keys for each year:")
    for t, y in media_items_years.items():
        no_s3_keys = [s3k for s3k, size in y.items() if size is None]
        valid_s3_keys = [s3k for s3k, size in y.items() if size is not None]
        logger.info(
            "\t[%s] has %s existing s3 keys and %s missing keys.",
            t,
            len(valid_s3_keys),
            len(no_s3_keys),
        )

    return media_items_years


def remove_media_in_manifest(mnf_json: dict[str, Any], white_list: list[str]) -> None:
    """
    Removes media items from the given manifest JSON object based on a whitelist.
    Typical use case is ingestion or processing only part of the media for whatever reason.

    Parameters:
        mnf_json (dict[str, Any]): The manifest JSON object containing a 'media_list'.
        white_list (list[str]): A list of media titles to be retained in the manifest.

    Returns:
        None: Modifies the input manifest JSON object in-place by removing media items
        not in the whitelist.
    """
    new_media_list = [
        media_item
        for media_item in mnf_json["media_list"]
        if media_item["media_title"] in white_list
    ]
    mnf_json["media_list"] = new_media_list
