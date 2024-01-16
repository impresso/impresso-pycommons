"""Helper functions to read ans write data versioning manifests
"""
import logging
import git
from enum import StrEnum

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



def get_git_credentials():
    return {
        'username': os.environ['GIT_USERNAME'],
        'password': os.environ['GIT_PASSWORD']
    }

def clone_git_repo(path: str, repo_name: str = "impresso/impresso-data-release"):
    repo_ssh_url = f"git@github.com:{repo_name}.git"
    repo_https_url = f"https://github.com/{repo_name}.git"
    # try to clone using ssh, if it fails, retry with https.
    try:
        return git.Repo.clone_from(repo_ssh_url, path)
    except Exception as e:
        logger.warning("Error while cloning the git repository using ssh,"
                       f" trying with https. \n{e}")
        pass  # Fallback to https
    try:
        return git.Repo.clone_from(repo_https_url, path)
    except Exception as e:
        logger.critical(f"Error while cloning the git repository. \n{e}")
        raise e


def validate_format(data_format: str) -> DataFormat | None:
    try:
        return DataFormat[data_format]
    except ValueError as e:
        logger.critical(f"{e} \nProvided data format '{data_format}'"
                        " is not a valid data format.")
        raise e

def validate_ganularity(value: str, for_stats: bool = True):
    lower = value.lower()
    if lower in POSSIBLE_GRANULARITIES:
        if for_stats and lower!= 'issue':
            return lower
    logger.critical(f"{e} \nProvided granularity '{lower}'"
                    " is not a valid granulartiy.")
    raise e


def read_manifest_contents() :
    # read and extract the contents of an arbitrary manifest, to be returned in dict format.
    pass

def list_and_date_s3_files():
    # return a list of (file, date) pairs from a given s3 bucket, 
    # to extract their last modification date if no previous manifest exists.
    pass

def push_to_git():
    # given the serialized json of a manifest, push it to the given subfolder on git
    pass

def upload_to_s3():
    # given the serialized manifest json of a manifest, upload it to the given bucket on S3.
    pass