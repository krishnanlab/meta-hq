"""
Settings for supported CLI options.

Author: Parker Hicks
Date: 2025-09-05
"""


def formats() -> list[str]:
    return ["parquet", "tsv", "csv", "json"]


def log_map() -> dict[str, int]:
    """Return mapping between log levels as text and their corresponding int values."""
    return {
        "notset": 0,
        "debug": 10,
        "info": 20,
        "warning": 30,
        "error": 40,
        "critical": 50,
    }


def required_filters() -> list[str]:
    return ["species", "technology", "ecode"]


def sample_ids() -> list[str]:
    return ["sample", "srr", "srx"]
