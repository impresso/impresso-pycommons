"""Helper functions to read ans write data versioning manifests
"""
import os
import logging
import git
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)

POSSIBLE_GRANULARITIES = ['collection', 'title', 'year', 'issue']

class DataFormat(StrEnum):

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

def validate_format(data_format: str) -> DataFormat | None:
    try:
        return DataFormat[data_format]
    except ValueError as e:
        logger.critical(f"{e} \nProvided data format '{data_format}'"
                        " is not a valid data format.")
        raise e

def validate_granularity(value: str, for_stats: bool = True):
    lower = value.lower()
    if lower in POSSIBLE_GRANULARITIES:
        if for_stats and lower!= 'issue':
            return lower
    logger.critical(f"{e} \nProvided granularity '{lower}'"
                    " is not a valid granulartiy.")
    raise e

def read_manifest_contents(man_dict: dict[str, Any]) :
    # read and extract the contents of an arbitrary manifest, to be returned in dict format.
    pass

def list_and_date_s3_files():
    # return a list of (file, date) pairs from a given s3 bucket, 
    # to extract their last modification date if no previous manifest exists.
    pass

def push_to_git():
    # given the serialized json of a manifest, push it to the given subfolder on git
    pass