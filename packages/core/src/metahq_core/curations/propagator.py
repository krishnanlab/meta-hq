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

Last updated: 2025-09-15 by Parker Hicks
"""

import multiprocessing as mp
from typing import TYPE_CHECKING, Literal

import numpy as np
import polars as pl
from tqdm import tqdm

from metahq_core.util.alltypes import NpIntMatrix, NpStringArray
from metahq_core.util.io import load_txt
from metahq_core.util.supported import onto_relations

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations


class MultiprocessPropagator:
    """Exists to allow multiprocessing within the Propagator class."""

    @staticmethod
    def _process_chunk(args):
        """
        Worker function for matrix dot product between annotation chunk
        and ontology relationship matrix.

        This is the function split between workers.
        """
        chunk_idx, chunk, family = args
        result = np.einsum("ij,jk->ik", chunk, family)
        return chunk_idx, result

    def multiprocess_propagate(
        self,
        n_indices,
        split: list,
        family: NpIntMatrix,
        relatives: Literal["ancestors", "descendants"],
        n_processes: int | None = None,
    ):
        """Multiprocessing propagation"""
        if n_processes is None:
            n_processes = mp.cpu_count()

        final_shape = (
            n_indices,
            family.shape[1],
        )

        propagated = np.empty(final_shape, dtype=np.int32)

        args_list = [(i, chunk, family) for i, chunk in enumerate(split)]

        with mp.Pool(processes=n_processes) as pool:
            results = list(
                tqdm(
                    pool.imap(self._process_chunk, args_list),
                    total=len(args_list),
                    desc=f"Propagating {relatives} in {len(args_list)} batches",
                )
            )

        # combine
        start = 0
        for _, result in sorted(results, key=lambda x: x[0]):
            nsamples = result.shape[0]
            end = start + nsamples
            propagated[start:end] = result
            start = end

        return propagated


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

    def __init__(self, ontology, anno, to_terms, relatives):
        self.ontology: str = ontology
        self.anno: Annotations = anno
        self.to: list[str] = to_terms

        self.family: dict[str, NpIntMatrix | NpStringArray] = {}
        self._relatives: list[str] = relatives
        self._load_family()

        self._propagator = MultiprocessPropagator()

    def propagate_down(self) -> tuple[NpIntMatrix, list[str], pl.DataFrame]:
        """Propagates annotations down to the terms in self.to"""
        return self._propagate_to_family("descendants")

    def propagate_up(self) -> tuple[NpIntMatrix, list[str], pl.DataFrame]:
        """Propagates annotations up to the terms in self.to"""
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
        tmp = np.array(load_txt(onto_relations(self.ontology, "ids")))
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
        self, relatives: Literal["ancestors", "descendants"]
    ) -> tuple[NpIntMatrix, list[str], pl.DataFrame]:
        """Multiprocess propagate to ancestors or descendants."""
        split = self._split_anno()
        propagated = self._propagator.multiprocess_propagate(
            self.anno.n_indices, split, self.family[relatives], relatives
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


def _process_groups(args):
    controls, groups, labels, label_ids, diseases, index_col, group_col = args
    ctrl_dfs = []
    for group in groups:
        _controls = controls.filter(pl.col(group_col) == group)[index_col].to_list()
        ctrl_anno = np.zeros((len(_controls), len(diseases)), dtype=np.int32)

        _anno = labels[np.where(np.array(label_ids[group_col]) == group)[0], :]
        positive_annos = np.any(_anno == 1, axis=0)

        ctrl_anno[:, positive_annos] = 2

        ctrl_dfs.append(
            controls.filter(pl.col(index_col).is_in(_controls))
            .select(index_col)
            .hstack(pl.DataFrame(ctrl_anno, schema=diseases))
        )

    return pl.concat(ctrl_dfs)


def propagate_controls(
    controls,
    labels,
    label_ids,
    diseases,
    index_col,
    group_col,
    n_processes: int | None = None,
):
    """
    labels: Labels
        Propagated labels with -1, 0, +1 labels to disease terms.

    controls: pl.DataFrame
        DataFrame of index and group IDs of indices annotated as controls
        in the original annotations matrix. Note the index and group columns
        must match those of the label IDs.
    """
    if n_processes is None:
        n_processes = mp.cpu_count()

    groups = controls[group_col].unique().to_list()
    nchunks = len(groups) // 50
    if nchunks == 0:
        nchunks = 1

    split = np.array_split(groups, nchunks)

    args_list = [
        (controls, _split, labels, label_ids, diseases, index_col, group_col)
        for _split in split
    ]

    with mp.Pool(processes=n_processes) as pool:
        results = list(
            tqdm(
                pool.imap(_process_groups, args_list),
                total=len(args_list),
                desc=f"Propagating controls in {len(args_list)} batches",
            )
        )

    return pl.concat(results)
