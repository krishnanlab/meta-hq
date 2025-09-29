"""
Settings for supported CLI options.

Author: Parker Hicks
Date: 2025-09-05
"""


def formats() -> list[str]:
    return ["parquet", "tsv", "csv", "json"]


def required_filters() -> list[str]:
    return ["species", "technology", "ecode"]


def sample_ids() -> list[str]:
    return ["sample", "srr", "srx"]
