"""
Helper functions for the MetaHQ CLI pipelines.

Author: Parker Hicks
Date: 2025-09

Last updated: 2025-09-24 by Parker Hicks
"""

from metahq_cli.util.messages import error
from metahq_cli.util.supported import required_filters


class FilterParser:
    """Class to parse and return metahq retrieve <attribute> filters."""

    def __init__(self):
        self.filters = {}

    @classmethod
    def from_dict(cls, config):
        raise NotImplementedError

    @classmethod
    def from_str(cls, filters: str):
        parser = cls()
        as_list: list[list[str]] = [f.split("=") for f in filters.split(",")]
        as_dict: dict[str, str] = {f[0]: f[1] for f in as_list}

        parser.filters = as_dict

        not_in_filters = []
        for key in required_filters():
            if key not in parser.filters:
                not_in_filters.append(key)

        if len(not_in_filters) > 0:
            error(f"Missing required filters {not_in_filters}.")
        return parser
