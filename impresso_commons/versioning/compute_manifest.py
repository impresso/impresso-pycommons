"""Command-line script to generate a manifest for an S3 bucket or partition after a processing.

Usage:
    compute_manifest.py --config-file=<cf> --log-file=<lf> [--scheduler=<sch> --nworkers=<nw> --verbose]

Options:

--config-file=<cf>  Path to configuration json file containing all necessary arguments for the computation of the manifest.
--log-file=<lf>  Path to log file to use.
--scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
--nworkers=<nw>  number of workers for (local) Dask client.
--verbose  Set logging level to DEBUG (by default is INFO).
"""

import json
import os
import traceback
import logging
from typing import Any, Optional
import git
from docopt import docopt

import dask.bag as db
from dask.distributed import Client
from impresso_commons.utils.s3 import fixed_s3fs_glob, IMPRESSO_STORAGEOPT
from impresso_commons.utils.utils import init_logger
from impresso_commons.versioning.helpers import (
    validate_stage,
    DataStage,
    compute_stats_in_canonical_bag,
    compute_stats_in_rebuilt_bag,
    compute_stats_in_entities_bag,
    compute_stats_in_langident_bag,
)
from impresso_commons.versioning.data_manifest import DataManifest

logger = logging.getLogger(__name__)

# list of optional configurations
OPT_CONFIG_KEYS = [
    "input_bucket",
    "newspapers",
    "previous_mft_s3_path",
    "is_patch",
    "patched_fields",
    "only_counting",
    "push_to_git",
    "notes",
]
# list of requirec configurations
REQ_CONFIG_KEYS = [
    "data_stage",
    "output_bucket",
    "git_repository",
    "is_staging",
    "file_extensions",
]


def extract_np_key(s3_key: str, bucket: str) -> str:
    """Extract the newspaper an s3:key corresponds to given the bucket and partition

    eg. s3_key is in format:
    - s3_key: 's3://31-passim-rebuilt-staging/passim/indeplux/indeplux-1889.jsonl.bz2'
    - bucket: '31-passim-rebuilt-staging/passim'
    --> returns 'indeplux'

    Args:
        s3_key (str): Full S3 path of a file (as returned by fixed_s3fs_glob).
        bucket (str): S3 bucket, including partition, in which the newspaper dirs are.

    Returns:
        str: Name of the corresponding newspaper, extracted form the s3 path.
    """
    # in format: 's3://31-passim-rebuilt-staging/passim/indeplux/indeplux-1889.jsonl.bz2'
    key_no_bucket = s3_key.replace(f"s3://{bucket}/", "")
    # Not all buckets separate the data per title, but the title will always come first.
    if "/" in key_no_bucket:
        return key_no_bucket.split("/")[0]
    else:
        return key_no_bucket.split("-")[0]


def get_files_to_consider(config: dict[str, Any]) -> Optional[dict[str, list[str]]]:
    """Get the list of S3 files to consider based on the provided configuration.

    Args:
        config (dict[str, Any]): Configuration parameters with the s3 bucket, titles,
            and file extensions

    Returns:
        dict[str, list[str]] | None: Dict mapping each newspaper to the s3 files to
            consider, or None if no files found.

    Raises:
        ValueError: If `file_extensions` in the config is empty or None.
    """
    if config["file_extensions"] == "" or config["file_extensions"] is None:
        raise ValueError("Config file's `file_extensions` should not be empty or None.")

    ext = config["file_extensions"]
    extension_filter = f"*{ext}" if "." in ext else f"*.{ext}"

    # if newspapers is empty, include all newspapers
    if config["newspapers"] is None or len(config["newspapers"]) == 0:
        logger.info("Fetching the files to consider for all titles...")
        # TODO update list_newspapers to include possibility of partition, and unify both cases
        # return all filenames in the given bucket partition with the correct extension
        files = fixed_s3fs_glob(os.path.join(config["output_bucket"], extension_filter))
        s3_files = {}
        for s3_key in files:
            np = extract_np_key(s3_key, config["output_bucket"])
            if np in s3_files:
                s3_files[np].append(s3_key)
            else:
                s3_files[np] = [s3_key]
        return s3_files

    # here list newspapers instead and s3_files becomes a dict np -> liest of files
    logger.info("Fetching the files to consider for titles %s...", config["newspapers"])
    s3_files = {}
    for np in config["newspapers"]:
        s3_files[np] = fixed_s3fs_glob(
            os.path.join(config["output_bucket"], np, extension_filter)
        )

    return s3_files


def compute_stats_for_stage(
    files_bag: db.core.Bag, stage: DataStage, client: Optional[Client] = None
) -> Optional[list[dict]]:
    """Compute statistics for a specific data stage.

    Args:
        files_bag (db.core.Bag): A bag containing files for statistics computation.
        stage (DataStage): The data stage for which statistics are computed.
        client (Client | None, optional): Dask client to use.

    Returns:
        list[dict] | None]: List of computed yearly statistics, or None if statistics
            computation for the given stage is not implemented.
    """
    match stage:
        case DataStage.CANONICAL:
            return compute_stats_in_canonical_bag(files_bag, client=client)
        case DataStage.REBUILT:
            return compute_stats_in_rebuilt_bag(
                files_bag, include_np=True, client=client
            )
        case DataStage.ENTITIES:
            return compute_stats_in_entities_bag(files_bag, client=client)
        case DataStage.PASSIM:
            return compute_stats_in_rebuilt_bag(
                files_bag, include_np=True, passim=True, client=client
            )
        case DataStage.LANGIDENT:
            return compute_stats_in_langident_bag(files_bag, client=client)
    raise NotImplementedError(
        "The function computing statistics for this DataStage is not yet implemented."
    )


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    """Ensure all required configurations are defined, add any missing optional ones.

    Args:
        config (dict[str, Any]): Provided configuration dict to compute the manifest.

    Raises:
        ValueError: Some required arguments of the configuration are missing.

    Returns:
        dict[str, Any]: Updated config, with any mssing optional argument set to None.
    """
    logger.info(
        "Validating that the provided configuration has all required arugments."
    )
    if not all([k in config for k in REQ_CONFIG_KEYS]):
        raise ValueError(f"Missing some required configurations: {REQ_CONFIG_KEYS}")

    for k in OPT_CONFIG_KEYS:
        if k not in config:
            logger.debug("%s was missing form the configuration, setting it to None", k)
            config[k] = None

    return config


def create_manifest(
    config_dict: dict[str, Any], client: Optional[Client] = None
) -> None:
    """Given its configuration, generate the manifest for a given s3 bucket partition.

    TODO: add option to agg for all titles together if desired
    TODO: add iptions to exclude NP for all agg types
    TODO: separate further into functions

    Note:
        The contents of the configuration file (or dict) are given in markdown file
        `impresso_commons/data/manifest_config/manifest.config.example.md``

    Args:
        config_dict (dict[str, Any]): Configuration following the guidelines.
        client (Client | None, optional): Dask client to use.
    """
    # if the logger was not previously inialized, do it
    if not logger.hasHandlers():
        init_logger()

    # ensure basic validity of the provided configuration
    config_dict = validate_config(config_dict)
    stage = validate_stage(config_dict["data_stage"])
    logger.info("Provided config validated.")

    logger.info("Starting to generate the manifest for DataStage: '%s'", stage)

    # fetch the names of the files to consider separated per title
    s3_files = get_files_to_consider(config_dict)

    logger.info(
        "Collected a total of %s files, reading them...", len(s3_files.values())
    )

    logger.info("Files identified successfully, initialising the manifest.")
    # init the git repo object for the processing's repository.
    repo = git.Repo(config_dict["git_repository"])

    # ideally any not-defined param should be None.
    in_bucket = config_dict["input_bucket"]
    p_fields = config_dict["patched_fields"]
    if p_fields is not None and len(p_fields) == 0:
        p_fields = None
    prev_mft = config_dict["previous_mft_s3_path"]
    only_counting = None
    if "only_counting" in config_dict:
        only_counting = config_dict["only_counting"]

    # init the manifest given the configuration
    manifest = DataManifest(
        data_stage=stage,
        s3_output_bucket=config_dict["output_bucket"],
        s3_input_bucket=in_bucket if in_bucket != "" else None,
        git_repo=repo,
        temp_dir=config_dict["temp_directory"],
        staging=config_dict["is_staging"],
        is_patch=config_dict["is_patch"],
        patched_fields=p_fields,
        previous_mft_path=prev_mft if prev_mft != "" else None,
        only_counting=only_counting,
    )

    # processing newspapers one at a time
    for np_title, np_s3_files in s3_files.items():

        logger.info("---------- %s ----------", np_title)
        logger.debug(
            "The list of files selected for %s is: %s",
            np_title,
            np_s3_files,
        )
        # load the selected files in dask bags
        processed_files = db.read_text(
            np_s3_files, storage_options=IMPRESSO_STORAGEOPT
        ).map(json.loads)

        logger.info(
            "%s - Starting to compute the statistics on the fetched files...",
            np_title,
        )
        computed_stats = compute_stats_for_stage(processed_files, stage, client)

        logger.info(
            "%s - Populating the manifest with the resulting %s yearly statistics found...",
            np_title,
            len(computed_stats),
        )
        logger.debug("%s - computed_stats: %s", np_title, computed_stats)

        for stats in computed_stats:
            title = stats["np_id"]
            year = stats["year"]
            del stats["np_id"]
            del stats["year"]
            logger.debug("Adding %s to %s-%s", stats, title, year)
            manifest.add_by_title_year(title, year, stats)

        logger.info("%s - Finished adding stats, going to the next title...", np_title)

    logger.info("Finalizing the manifest, and computing the result...")
    # Add the note to the manifest
    if config_dict["notes"] is not None and config_dict["notes"] != "":
        manifest.append_to_notes(config_dict["notes"])
    else:
        note = f"Processing data to generate {stage} for "
        if len(config_dict["newspapers"]) != 0:
            note += f"titles: {config_dict['newspapers']}."
        else:
            note += "all newspaper titles."

        manifest.append_to_notes(note)

    if config_dict["push_to_git"]:
        manifest.compute(export_to_git_and_s3=True)
    else:
        manifest.compute(export_to_git_and_s3=False)
        manifest.validate_and_export_manifest(push_to_git=False)


def main():
    arguments = docopt(__doc__)
    config_file_path = arguments["--config-file"]
    log_file = arguments["--log-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    nworkers = int(arguments["--nworkers"]) if arguments["--nworkers"] else 8
    scheduler = arguments["--scheduler"]

    init_logger(log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    # start the dask local cluster
    if scheduler is None:
        client = Client(n_workers=nworkers, threads_per_worker=1)
    else:
        client = Client(scheduler)

    dask_cluster_msg = f"Dask local cluster: {client}"
    logger.info(dask_cluster_msg)
    print(dask_cluster_msg)

    logger.info("Reading the arguments inside %s", config_file_path)
    with open(config_file_path, "r", encoding="utf-8") as f_in:
        config_dict = json.load(f_in)

    try:
        logger.info("Provided configuration: ")
        logger.info(config_dict)
        create_manifest(config_dict, client)

    except Exception as e:
        traceback.print_tb(e.__traceback__)
        print(e)
        client.shutdown()


if __name__ == "__main__":
    main()
