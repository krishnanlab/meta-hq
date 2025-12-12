import functools
import operator
from typing import Any

from metahq_core.util.alltypes import StringArray


def flatten_list(l: list[list[Any]]) -> list[Any]:
    """Flattens a list of lists."""
    return functools.reduce(operator.iconcat, l, [])


def reverse_dict(d: dict) -> dict:
    """Sets values as keys and keys as values."""
    _d = {}
    for key, val in d.items():
        if isinstance(val, list):
            for item in val:
                _d[item] = key

        else:
            _d[val] = key
    return _d


def subset_keys(dict_: dict[str, Any], subset: StringArray) -> dict[str, Any]:
    """Subset a dictionary by top level keys."""
    return {key: dict_[key] for key in subset if key in dict_}


def merge_list_values(dict_: dict[str, list[str]]):
    """Combine all lists that are values of a dictionary to a single list."""
    merged = []
    for value in dict_.values():
        merged.extend(value)
    return merged
