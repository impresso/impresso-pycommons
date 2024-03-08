"""Command-line script to generate a manifest for an S3 bucket or partition after a processing.

Usage:
    compute_manifest.py --config-file=<cf> [--verbose]

Options:

--config-file=<cf>  Path to configuration json file containing all necessary arguments for the computation of the manifest.
--verbose  Set logging level to DEBUG (by default is INFO)
"""

import json
import os
import git
from docopt import docopt
from time import strftime
import copy
from typing import Any
import logging

import dask.bag as db

from impresso_commons.utils.s3 import upload, fixed_s3fs_glob, IMPRESSO_STORAGEOPT
from impresso_commons.path.path_s3 import fetch_files
from impresso_commons.text.rebuilder import init_logging
from impresso_commons.versioning.data_statistics import (
    NewspaperStatistics,
)
from impresso_commons.versioning.helpers import (
    validate_stage,
    DataStage,
    compute_stats_in_canonical_bag,
    compute_stats_in_rebuilt_bag,
    compute_stats_in_entities_bag,
    write_dump_to_fs
)
from impresso_commons.versioning.data_manifest import DataManifest

logger = logging.getLogger(__name__)

def get_files_to_consider(config: dict[str, Any]) -> list[str] | None:
    """Get the list of S3 files to consider based on the provided configuration.

    Args:
        config (dict[str, Any]): Configuration parameters with the s3 bucket, titles, 
            and file extensions

    Returns:
        list[str] | None: List of the s3 files to consider, or None if no files found.
        
    Raises:
        ValueError: If `file_extensions` in the config is empty or None.
    """
    if config["file_extensions"] == "" or config["file_extensions"] is None:
        raise ValueError("Config file's `file_extensions` should not be empty or None.")

    ext = config["file_extensions"]
    extension_filter = f"*{ext}" if ext.startswith(".") else f"*.{ext}"
    
    # if newspapers is empty, include all newspapers
    if len(config["newspapers"]) == 0:
        # return all filenames in the given bucket partition with the correct extension
        return fixed_s3fs_glob(os.path.join(config["output_bucket"], extension_filter))

    s3_files = []
    for np in config["newspapers"]:
        s3_files.extend(
            fixed_s3fs_glob(os.path.join(config["output_bucket"], np, extension_filter))
        )

    return s3_files


def compute_stats_for_stage(
    files_bag: db.core.Bag, stage: DataStage
) -> list[dict] | None:
    """Compute statistics for a specific data stage.

    Args:
        files_bag (db.core.Bag): A bag containing files for statistics computation.
        stage (DataStage): The data stage for which statistics are computed.

    Returns:
        list[dict] | None]: List of computed yearly statistics, or None if statistics 
            computation for the given stage is not implemented.
    """
    match stage:
        case DataStage.CANONICAL:
            return compute_stats_in_canonical_bag(files_bag)
        case DataStage.REBUILT:
            return compute_stats_in_rebuilt_bag(files_bag)
        case DataStage.ENTITIES:
            return compute_stats_in_entities_bag(files_bag)
    raise NotImplementedError(
        "The function computing statistics for this DataStage is not yet implemented."
    )


def main():
    arguments = docopt(__doc__)
    config_file_path = arguments["--config-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    logger.info(f"Reading the arguments inside {config_file_path}")
    with open(config_file_path, "r", encoding="utf-8") as f_in:
        config_dict = json.load(f_in)

    init_logging(log_level, config_dict['log_file'])

    # ensure that the provided Data stage is correct
    stage = validate_stage(config_dict["data_stage"])
    logger.info(f"Starting to generate the manifest for DataStage: '{stage}'")
    
    logger.info("Fetching the files to consider...")
    # fetch the names of the files to consider
    s3_files = get_files_to_consider(config_dict)
    # load the selected files in dask bags
    processed_files = db.read_text(s3_files, storage_options=IMPRESSO_STORAGEOPT).map(
        json.loads
    )

    # init the git repo object for the processing's repository.
    repo = git.Repo(config_dict["git_repository"])

    # ideally any not-defined param should be None.
    in_bucket = config_dict["input_bucket"]
    p_fields = config_dict["patched_fields"]
    if p_fields is not None and len(p_fields) == 0:
        p_fields = None
    prev_mft = config_dict["previous_mft_s3_path"]

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
    )

    logger.info("Starting to compute the statistics on the fetches files...")
    computed_stats = compute_stats_for_stage(processed_files, stage)

    logger.info("Populating the manifest with the resulting yearly statistics...")
    for stats in computed_stats:
        title = stats["np_id"]
        year = stats["year"]
        del stats["np_id"]
        del stats["year"]
        manifest.add_by_title_year(title, year, stats)

    logger.info("Finalizing the manifest, and computing the result...")

    note = "Rebuilt of newspaper articles for "
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

if __name__ == "__main__":
    main()
