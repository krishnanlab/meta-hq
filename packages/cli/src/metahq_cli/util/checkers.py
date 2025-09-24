"""
Checker functions for the MetaHQ CLI pipelines.

Author: Parker Hicks
Date: 2025-09

Last updated: 2025-09-24 by Parker Hicks
"""

from metahq_cli.util.supported import REQUIRED_FILTERS


def check_filters(filters: dict[str, str]):
    unaccaptable = []
    for f in filters:
        if f not in REQUIRED_FILTERS:
            unaccaptable.append(f)
    return unaccaptable
