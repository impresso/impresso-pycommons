#!/usr/bin/env python
# coding: utf-8

import json
import os
import logging
import pathlib
from contextlib import ExitStack
from typing import Any, Optional
import jsonschema
import importlib_resources

logger = logging.getLogger(__name__)


def parse_json(filename):
    if os.path.isfile(filename):
        with open(filename, "r") as f:
            return json.load(f)
    else:
        logger.info(f"File {filename} does not exist.")


def chunk(list, chunksize):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(list), chunksize):
        yield list[i : i + chunksize]


def get_pkg_resource(
    file_manager: ExitStack, path: str, package: str = "impresso_commons"
) -> pathlib.PosixPath:
    """Return the resource at `path` in `package`, using a context manager.

    Note:
        The context manager `file_manager` needs to be instantiated prior to
        calling this function and should be closed once the package resource
        is no longer of use.

    Args:
        file_manager (contextlib.ExitStack): Context manager.
        path (str): Path to the desired resource in given package.
        package (str, optional): Package name. Defaults to "impresso_commons".

    Returns:
        pathlib.PosixPath: Path to desired managed resource.
    """
    ref = importlib_resources.files(package) / path
    return file_manager.enter_context(importlib_resources.as_file(ref))


def init_logger(
    level: int = logging.INFO, file: Optional[str] = None
) -> logging.RootLogger:
    """Initialises the root logger.

    Args:
        level (int, optional): desired level of logging. Defaults to logging.INFO.
        file (str | None, optional): _description_. Defaults to None.

    Returns:
        logging.RootLogger: the initialised logger
    """
    # Initialise the logger
    root_logger = logging.getLogger("")
    root_logger.setLevel(level)

    if file is not None:
        handler = logging.FileHandler(filename=file, mode="w")
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.info("Logger successfully initialised")

    return root_logger


def validate_against_schema(
    json_to_validate: dict[str, Any],
    path_to_schema: str = "schemas/json/versioning/manifest.schema.json",
) -> None:
    """Validate a dict corresponding to a JSON against a provided JSON schema.

    Args:
        json (dict[str, Any]): JSON data to validate against a schema.
        path_to_schema (str, optional): Path to the JSON schema to validate against.
            Defaults to "impresso-schemas/json/versioning/manifest.schema.json".

    Raises:
        e: The provided JSON could not be validated against the provided schema.
    """
    file_manager = ExitStack()
    schema_path = get_pkg_resource(file_manager, path_to_schema)
    with open(os.path.join(schema_path), "r", encoding="utf-8") as f:
        json_schema = json.load(f)

    try:
        jsonschema.validate(json_to_validate, json_schema)
    except Exception as e:
        logger.error(
            "The provided JSON could not be validated against its schema: %s.",
            json_to_validate,
        )
        raise e


def bytes_to(bytes_nb: int, to_unit: str, bsize: int = 1024) -> float:
    """Convert bytes to the specified unit.

    Supported target units:
    - 'k' (kilobytes), 'm' (megabytes),
    - 'g' (gigabytes), 't' (terabytes),
    - 'p' (petabytes), 'e' (exabytes).

    Args:
        bytes_nb (int): The number of bytes to be converted.
        to_unit (str): The target unit for conversion.
        bsize (int, optional): The base size used for conversion (default is 1024).

    Returns:
        float: The converted value in the specified unit.

    Raises:
        KeyError: If the specified target unit is not supported.
    """
    units = {"k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6}
    return float(bytes_nb) / (bsize ** units[to_unit])
