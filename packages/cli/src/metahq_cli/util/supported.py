"""
Settings for supported CLI options.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-11-20 by Parker Hicks
"""

from pathlib import Path

LATEST_VERSION: str = "v1.0.0-alpha"


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


def zenodo_records_url() -> str:
    """Return the url to Zenodo records."""
    return "https://zenodo.org/records"


def zenodo_files_dir() -> str:
    """Return the directory name for Zenodo files for a particular record."""
    return "files"


def metahq_dois(doi: str) -> dict[str, str]:
    """
    Return version and filename information for a MetaHQ database Zenodo DOI.

    Parameters
    ----------
    doi: str
        A valid Zenodo DOI for a MetaHQ database version.

    Returns
    -------
    Database version and the filename for the particular DOI.

    """
    dois = {
        "17663087": {"version": "v1.0.0-alpha", "filename": "metahq.tar.gz"},
        "17666183": {"version": "v1.0.1-alpha", "filename": "metahq_data.tar.gz"},
    }
    if not doi in dois:
        available = list(dois.keys())
        raise ValueError(f"Expected doi in {available}, got {dois}.")

    return dois[doi]


def metahq_mother_dir() -> Path:
    """Return the name of the MetaHQ directory for every user.

    This directory will store the package config file and logs.
    """
    return Path.home() / "metahq"


def default_data_dir() -> Path:
    """Return the default name of the data directory."""
    return Path.home() / (".metahq_data")
