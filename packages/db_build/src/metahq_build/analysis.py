"""
This module contains functions to use in analysis in the `scripts` and `notebook` directories.

Author: Parker Hicks
Date: 2026-06-04
"""

import numpy as np
from numpy.typing import NDArray


def get_source_contribution_overlap(
    contributions: dict[str, set[str]],
) -> dict[str, NDArray]:
    """Given a map of sources to sample or study IDs, compute the absolute
    and percent overlap between them.

    Arguments:
        contributions (dict[str, set[str]]):
            Mapping of sources to sample or study IDs.

    Returns:
        (dict[str, NDArray]): Dictionary storing arrays of column/row names (sources),
            the absolute counts overlap and the percent overlap.
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

    return {
        "sources": source_names,
        "overlap_count": source_overlap_counts,
        "overlap_percent": source_overlap_percent,
    }
