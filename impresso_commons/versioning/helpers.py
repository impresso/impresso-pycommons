"""Helper functions to read ans write data versioning manifests
"""
import os
import logging
import git
import boto3 
import json

from enum import StrEnum
from typing import Any
from impresso_commons.utils.s3 import (fixed_s3fs_glob, alternative_read_text,
                                       get_storage_options, get_boto3_bucket)

logger = logging.getLogger(__name__)

POSSIBLE_GRANULARITIES = ['corpus', 'title', 'year', 'issue']
IMPRESSO_STORAGEOPT = get_storage_options()

class DataStage(StrEnum):

    canonical = 'canonical'
    rebuilt = 'rebuilt'
    embeddings = 'embeddings'
    entities = 'entities'
    langident = 'langident'
    linguistic_processing = 'linguistic-processing'
    mentions = 'mentions'
    orcqa = 'orcqa'
    text_reuse = 'text-reuse'
    topics = 'topics'

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_ 



"""def get_git_credentials():
    return {
        'username': os.environ['GIT_USERNAME'],
        'password': os.environ['GIT_PASSWORD']
    }"""

def is_git_repo(path):
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False

def clone_git_repo(path: str, repo_name: str = "impresso/impresso-data-release",
                   branch: str = 'master') -> git.Repo:
    repo_ssh_url = f"git@github.com:{repo_name}.git"
    repo_https_url = f"https://github.com/{repo_name}.git"

    repo_path = os.path.join(path, repo_name.split('/')[1])

    # if the repository was already cloned, return it.
    if is_git_repo(repo_path):
        logger.debug(f"Git repository {repo_name} had already been cloned.")
        return git.Repo(repo_path)

    # try to clone using ssh, if it fails, retry with https.
    try:
        logger.info(f"Cloning the {repo_name} git repository with ssh.")
        return git.Repo.clone_from(repo_ssh_url, repo_path, branch = branch)
    except Exception as e:
        logger.warning("Error while cloning the git repository using ssh,"
                       f" trying with https. \n{e}")
        pass  # Fallback to https
    try:
        logger.info(f"Cloning the {repo_name} git repository with https.")
        return git.Repo.clone_from(repo_https_url, repo_path, branch = branch)
    except Exception as e:
        logger.critical(f"Error while cloning the git repository, it was not "
                        f"possible to clone it with ssh or https. \n{e}")
        raise e

def validate_format(data_format: str) -> DataStage | None:
    try:
        return DataStage[data_format]
    except ValueError as e:
        logger.critical(f"{e} \nProvided data format '{data_format}'"
                        " is not a valid data format.")
        raise e

def validate_granularity(value: str, for_stats: bool = True):
    lower = value.lower()
    if lower in POSSIBLE_GRANULARITIES:
        if not for_stats or (for_stats and lower!= 'issue'):
            return lower
    # only incorrect granularity values will not be returned
    logger.critical(f"Provided granularity '{lower}' isn't a valid.")
    raise ValueError

def extract_version(name_or_path: str, as_int: bool=False) -> list[str | int]:
    # in the case it's a path
    basename = os.path.basename(name_or_path)
    version = basename.replace('.json', '').split('_')[-1]
    if as_int:
        return int(version[1:].replace('-', ''))
    else:
        return version.replace('-', '.')

def find_s3_data_manifest_path(bucket, data_format: str) -> str:
    # manifests have a json and are named after the format
    path_filter = f"{data_format}_v*.json"
    # processed data are all in the same bucket
    if data_format not in ['canonical', 'rebuilt']:
        path_filter = os.path.join(data_format, path_filter)
    
    matches = fixed_s3fs_glob(path_filter, boto3_bucket=bucket)
    # matches will always be a list
    if len(matches) == 1:
        return matches[0]
    else:
        # if multiple versions exist, return the latest one
        return sorted(matches, 
                      key = lambda x: extract_version(x, as_int=True))[-1]

def read_manifest_from_s3(
    bucket_name: str, data_format: str
) -> tuple[str, dict[str, Any]]:
    # read and extract the contents of an arbitrary manifest, to be returned in dict format.
    bucket = get_boto3_bucket(bucket_name)
    manifest_s3_path = find_s3_data_manifest_path(bucket, data_format)
    
    raw_text = alternative_read_text(manifest_s3_path, 
                                     IMPRESSO_STORAGEOPT, 
                                     line_by_line=False)

    return manifest_s3_path, json.loads(raw_text)


def list_and_date_s3_files():
    # return a list of (file, date) pairs from a given s3 bucket, 
    # to extract their last modification date if no previous manifest exists.
    pass

def write_dump_to_fs(
    file_contents: str, abs_path: str, filename: str
) -> str:
    full_file_path = os.path.join(abs_path, filename)
    
    # write file to absolute path provided
    with open(full_file_path, "w") as outfile:
        outfile.write(file_contents)

    return full_file_path

def write_and_push_to_git(
    file_contents: str, git_repo: git.Repo, path_in_repo: str, 
    filename: str, commit_msg: str | None = None) -> tuple[bool, str]:
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
            commit_msg = f'Add generated manifest file {filename}.'
        git_repo.index.commit(commit_msg)
        origin = git_repo.remote(name='origin')
        logger.info(f"Pushing {filename} with commit message {commit_msg}.")
        origin.push()  
        return True
    except Exception as e:
        logger.error(f"Error while pushing {filename}. \n{e}")
        return False