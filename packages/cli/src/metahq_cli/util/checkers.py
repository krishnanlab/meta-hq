"""
Checker functions for the MetaHQ CLI pipelines.

These checkers pull from two separate supported modules from
metahq_core and metahq_cli since there are different requirements
for CLI-based checks and MetaHQ core function checks.

Author: Parker Hicks
Date: 2025-09

Last updated: 2025-09-29 by Parker Hicks
"""

from pathlib import Path

from metahq_core.util.io import checkdir
from metahq_core.util.supported import supported

from metahq_cli.util.messages import error
from metahq_cli.util.supported import formats, required_filters


def check_filters(filters: dict[str, str]):
    unaccaptable = []
    for f in filters:
        if f not in required_filters():
            unaccaptable.append(f)
    return unaccaptable


def check_level(level: str):
    if level not in supported("levels"):
        error(f"Expected level in {supported('levels')}, got {level}.")


def check_metadata(level: str, metadata: str):
    _metadata = metadata.split(",")

    if level == "sample":
        _supported = supported("sample_metadata")

    elif level == "series":
        _supported = supported("series_metadata")

    else:
        check_level(level)
        exit(0)

    report_bad_entries("metadata", _supported, _metadata)


def check_format(fmt: str):
    _supported = formats()
    if fmt not in _supported:
        error(f"Expected fmt argument in {_supported}, got {fmt}.")


def check_mode(task: str, mode: str):
    if (task == "sex") & (mode != "direct"):
        error(
            "Sex annotation queries must be direct annotations. Change to mode argument to 'direct'."
        )


def check_outfile(outfile: Path | str):
    _ = checkdir(outfile, is_file=True)


def report_bad_entries(field: str, _supported: list[str], entries: list[str]):
    bad = []
    for entry in entries:
        if entry not in _supported:
            bad.append(entry)

    if len(bad) > 0:
        error(f"Bad arguments for {field}: {bad}. Expected arguments in {_supported}.")
