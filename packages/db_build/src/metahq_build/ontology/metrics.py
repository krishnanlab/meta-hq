"""
Metrics for the analysis of ontologies.
"""

from dataclasses import dataclass
from math import log

import polars as pl


@dataclass(slots=True, frozen=True)
class ICResult:
    """Result of an information content computation."""

    name: str
    value: float


class ICResults:
    """Results from multiple information content computations across nodes."""

    def __init__(self, results: dict[str, float] | None = None):

        if results is None:
            self.results = {}

    def pl(self, index: str = "id", value: str = "ic") -> pl.DataFrame:
        """Convert the result to a polars DataFrame"""
        return pl.DataFrame(
            {index: list(self.results.keys()), value: list(self.results.values())}
        )

    @classmethod
    def from_list(cls, results: list[ICResult]) -> "ICResults":
        """Initialize a BatchICResult from a list of ICResults."""
        batch_results = cls()

        _results: dict[str, float] = {}
        for result in results:
            _results[result.name] = result.value

        batch_results.results = _results

        return batch_results


def information_content(n_descendants: int, n_nodes: int):
    """Compute the information content.

    Arguments:
        n_descendants (int):
            The number of descendants of a particular node.
        n_nodes (int):
            The total number of nodes in the graph.

    Returns:
        (float): Information content.
    """
    return -1 * log((n_descendants + 1) / n_nodes)
