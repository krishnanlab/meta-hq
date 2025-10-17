"""
Class for performing annotation propagation.

Assigns labels to terms by propagating annotations through
an ontology structure.

Applies the dot product between an annotations matrix and familial adjacency
matrices. Below is the computation:

    (samples x reference_terms) @ (reference_terms, propagated_terms)
        -> (samples x propagated_terms).

This is done once for ancestors and once for descendants. Then for each sample,
if a term is is not an ancestor or descendant of that sample, then the sample is
given a negative label for that term.


Author: Parker Hicks
Date: 2025-04-23

Last updated: 2025-10-16 by Parker Hicks
"""

from typing import TYPE_CHECKING, Literal

import numpy as np
import polars as pl

from metahq_core.curations._multiprocess_propagator import MultiprocessPropagator
from metahq_core.logger import setup_logger
from metahq_core.util.alltypes import NpIntMatrix, NpStringArray
from metahq_core.util.supported import onto_relations

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.annotations import Annotations


class Propagator:
    """
    Class to propagate annotations to labels given an ontology structure.

    Attributes
    ----------
    ontology: str
        The name of an ontology supported by MetaHQ.

    anno: Annotations
        A MetaHQ Annotations object with columns of ontology terms
        rows as samples, and each value is a 1 or 0 indicating if a sample is
        annotated to a particular term.

    to: list[str]
        A list of ontology term IDs to propagate annotations up or down to.

    family: dict[str, pl.DataFrame | list[str]]
        A pointer to the ancestry and descendants adjacency matrices and ids
        denoting their column ids.

    Methods
    -------
    propagate_up()
        Propagates annotations up to all terms in the annotations curation.
        If an index is annotated to a descendant of a term in `to`, then it
        is given an annotation of 1 to that term.

    propagate_down()
        Propagates annotations down to all terms in the annotations curation.
        If an index is annotated to an ancestor of a term in `to`, then it
        is given an annotation of 1 to that term.

    """

    def __init__(
        self,
        ontology,
        anno,
        to_terms,
        relatives,
        logger=setup_logger(__name__),
        verbose=True,
    ):
        self.ontology: str = ontology
        self.anno: Annotations = anno
        self.to: list[str] = to_terms

        self.family: dict[str, NpIntMatrix | NpStringArray] = {}
        self._relatives: list[str] = relatives
        self._load_family()

        self.log: logging.Logger = logger
        self.verbose: bool = verbose
        self._propagator = MultiprocessPropagator(logger=logger, verbose=verbose)

    def propagate_down(
        self, verbose: bool = False
    ) -> tuple[NpIntMatrix, list[str], pl.DataFrame]:
        """Propagates annotations down to the terms in self.to"""
        if verbose:
            return self._propagate_to_family(
                "descendants", task="Propagating descendants"
            )
        return self._propagate_to_family("descendants")

    def propagate_up(
        self, verbose: bool = False
    ) -> tuple[NpIntMatrix, list[str], pl.DataFrame]:
        """Propagates annotations up to the terms in self.to"""
        if verbose:
            return self._propagate_to_family("ancestors", task="Propagating ancestors")
        return self._propagate_to_family("ancestors")

    def _load_anscestors(
        self, lf: pl.LazyFrame, _from: list[str], all_terms: pl.Series
    ) -> NpIntMatrix:
        """
        Loads the relations matrix with a ancestor-forward orientation.

        Returns
        -------
        Matrix of shape [_from, _to] where each value indicates if a particular
        column is a ancestor of a particular row.

        """
        return (
            lf.select(_from)
            .with_columns(all_terms)
            .filter(pl.col("terms").is_in(self.to))
            .drop("terms")
            .collect()
            .transpose()
            .to_numpy()
        )

    def _load_descendants(
        self, lf: pl.LazyFrame, _from: list[str], all_terms: pl.Series
    ) -> NpIntMatrix:
        """
        Loads the relations matrix with a descendants-forward orientation.

        Returns
        -------
        Matrix of shape [_from, _to] where each value indicates if a particular
        column is a descendant of a particular row.

        """
        return (
            lf.select(self.to)
            .with_columns(all_terms)
            .filter(pl.col("terms").is_in(_from))
            .drop("terms")
            .collect()
            .to_numpy()
        )

    def _load_family(self):
        """
        Loads the terms x terms adjacency matrices for ancestor and descendant relationships.
        These matrices store column-wise relational annotations where if term_n is an ancestor
        of term_m, then ancestors[n, m] will be 1 and ancestors[m, n] will be 0. This matrix is
        transposed when loading to get row-wise relational annotations and match dimensions with
        `self.anno`.
        """
        self.family["ids"] = self._load_family_ids()

        for relatives in self._relatives:
            self.family[relatives] = self._load_relatives(relatives)

    def _load_family_ids(self) -> NpStringArray:
        """Loads the term IDs of the relations DataFrame."""
        tmp = (
            pl.scan_parquet(onto_relations(self.ontology, "relations"))
            .collect_schema()
            .names()
        )
        return np.array([term for term in tmp if term in self.to])

    def _load_relatives(self, relatives: str) -> NpIntMatrix:
        """Loads the relationships matrix between ontology terms."""
        lf = pl.scan_parquet(onto_relations(self.ontology, "relations"))
        all_terms = pl.Series("terms", lf.collect_schema().names())

        self.anno = self.anno.sort_columns()
        _from = self.anno.data.columns

        opt = {
            "ancestors": self._load_anscestors,
            "descendants": self._load_descendants,
        }
        if relatives in opt:
            return opt[relatives](lf, _from, all_terms)

        raise ValueError(f"Expected relatives in {list(opt.keys())}, got {relatives}.")

    def _propagate_to_family(
        self,
        relatives: Literal["ancestors", "descendants"],
        task: str = "Propagating",
    ) -> tuple[NpIntMatrix, list[str], pl.DataFrame]:
        """Multiprocess propagate to ancestors or descendants."""
        split = self._split_anno()

        propagated = self._propagator.multiprocess_propagate(
            self.anno.n_indices,
            split,
            self.family[relatives],
            desc=task,
        )
        propagated = np.where(propagated >= 1, 1, propagated)

        return propagated, list(self.family["ids"]), self.anno.ids

    def _split_anno(self) -> list:
        """
        Splits annotation matrix into chunks row-wise to reduce computational overhead
        for matrix multiplication. Each chunk will have at most 1000 entries.
        """
        nchunks = self.anno.ids.height // 500
        if nchunks == 0:
            nchunks = 1
        return np.array_split(self.anno.data.to_numpy().astype(np.float32), nchunks)
