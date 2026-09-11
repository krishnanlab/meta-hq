"""
This module contains functions to use in analysis in the `scripts` and `notebook` directories.

Author: Parker Hicks
Date: 2026-06-04

Last updated: 2026-06-05 by Parker Hicks
"""

from pathlib import Path
from typing import Literal, TypeAlias, get_args

import numpy as np
import polars as pl
from numpy.typing import NDArray

OverlapField: TypeAlias = Literal["count", "percent"]


class OverlapResult:
    def __init__(self, sources, count, percent):
        self.sources: NDArray = sources
        self.count: NDArray = count
        self.percent: NDArray = percent

    def pl(self, field: OverlapField) -> pl.DataFrame:
        """Return counts or percents overlap results as a polars DataFrame."""
        match field:
            case "count":
                return pl.DataFrame(self.count, schema=list(self.sources))
            case "percent":
                return pl.DataFrame(self.percent, schema=list(self.sources))

    def save_field(
        self,
        field: OverlapField,
        outfile: str | Path,
        separator: str = "\t",
    ) -> None:
        """Save a data field to a csv-like file."""
        self.pl(field).write_csv(outfile, separator=separator)

    @property
    def fields(self) -> set[OverlapField]:
        """Return available fields"""
        return set(get_args(OverlapField))


def get_source_contribution_overlap(
    contributions: dict[str, set[str]],
) -> OverlapResult:
    """Given a map of sources to sample or study IDs, compute the absolute
    and percent overlap between them.

    Arguments:
        contributions (dict[str, set[str]]):
            Mapping of sources to sample or study IDs.

    Returns:
        (OverlapResult)
    """
    source_names = np.array(list(contributions.keys()))  # column/row names
    source_overlap_counts = np.zeros(
        (len(contributions), len(contributions)), dtype=np.int64
    )  # discrete overlap counts
    source_overlap_percent = np.zeros(
        (len(contributions), len(contributions)), dtype=np.float64
    )  # counts normalized by total number of entries between sources

    # compute overlap
    for i_source, i_entries in contributions.items():
        for j_source, j_entries in contributions.items():
            total = len(
                i_entries | j_entries
            )  # total number of samples between the two sources
            intersection = len(i_entries & j_entries)
            intersection_percent = intersection / total

            i_idx = np.where(source_names == i_source)[0]
            j_idx = np.where(source_names == j_source)[0]

            source_overlap_counts[i_idx, j_idx] = intersection
            source_overlap_percent[i_idx, j_idx] = intersection_percent

    return OverlapResult(
        source_names, count=source_overlap_counts, percent=source_overlap_percent
    )
