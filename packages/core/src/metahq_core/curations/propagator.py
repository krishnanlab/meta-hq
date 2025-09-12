"""
Class for performing annotation propagation.

Author: Parker Hicks
Date: 2025-04-23

Last updated: 2025-09-01 by Parker Hicks
"""

import multiprocessing as mp
from typing import TYPE_CHECKING, Literal

import numpy as np
import polars as pl
from tqdm import tqdm

from metahq_core.util.alltypes import IdArray, NpIntMatrix, NpStringArray
from metahq_core.util.io import load_txt
from metahq_core.util.supported import onto_relations

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations


class MultiprocessPropagator:
    """Class-based version to allow multiprocessing within Propagator."""

    @staticmethod
    def _process_chunk_static(args):
        """Static method worker function"""
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
                    pool.imap(self._process_chunk_static, args_list),
                    total=len(args_list),
                    desc=f"Propagating {relatives} with {n_processes} processes",
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
    ontology: (str)
        The name of an ontology supported by MetaHQ.

    anno: (Annotations)
        A metahq.curations Annotations object with columns of ontology terms
        rows as samples, and each value is a 1 or 0 indicating if a sample is
        annotated to a particular term.

    family: (dict[str, pl.DataFrame | IdArray])
        A pointer to the ancestry and descendants adjacency matrices and ids
        denoting their column ids.

    _relatives: (list[str])
        Private attribute simplifying family dictionary iterations.

    Methods
    -------
    propagate()
        Propagates the passed annotations to all available terms or a specified subset.

    select()
        Subsets the ancetors and descendants adjacency matrices given specified
        subset of terms to propagate to.

    _load_family()
        Loads precomputed ancestor and descendants adjacency matrices.

    _vector_propagate()
        Performs a vectorized propagation of annotations.

    """

    def __init__(self, ontology, anno, to_terms, relatives):
        self.ontology: str = ontology
        self.anno: Annotations = anno
        self._to: list[str] = to_terms

        self.family: dict[str, NpIntMatrix | NpStringArray] = {}
        self._relatives: list[str] = relatives
        self._load_family()

        self.propagator = MultiprocessPropagator()

    def propagate(self) -> pl.DataFrame:
        """
        Assigns labels to terms by propagating annotations through
        an ontology structure.

        Parameters
        ----------
        to: (IdArray)
            Array of terms to propagate to.

        """
        self._vector_propagate()
        labels_df = pl.DataFrame(
            self.anno, schema=list(self.family["ids"]), orient="row"
        )

        return labels_df

    def select(self, terms: IdArray):
        """
        Filters the columns of the ancestors and descendants
        matrices for a specified subset.

        Parameters
        ----------
        Terms: (IdArray)
            Terms to propagate to.

        """
        new_terms = np.isin(self.family["ids"], terms)
        for relatives in self._relatives:
            self.family[relatives] = self.family[relatives][:, new_terms]

        self.family["ids"] = self.family["ids"][new_terms]

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
        tmp = np.array(load_txt(onto_relations(self.ontology, "ids")))
        return np.array([term for term in tmp if term in self._to])

    def _load_relatives(
        self, relatives: Literal["ancestors", "descendants"]
    ) -> NpIntMatrix:
        lf = pl.scan_parquet(onto_relations(self.ontology, "relations"))
        all_terms = pl.Series("terms", lf.collect_schema().names())

        self.anno = self.anno.sort_columns()
        _from = self.anno.data.columns

        opt = {
            "ancestors": self._load_anscestors,
            "descendants": self._load_descendants,
        }

        return opt[relatives](lf, _from, all_terms)

    def _load_anscestors(
        self, lf: pl.LazyFrame, _from: list[str], all_terms: pl.Series
    ) -> NpIntMatrix:
        return (
            lf.select(_from)
            .with_columns(all_terms)
            .filter(pl.col("terms").is_in(self._to))
            .drop("terms")
            .collect()
            .transpose()
            .to_numpy()
        )

    def _load_descendants(
        self, lf: pl.LazyFrame, _from: list[str], all_terms: pl.Series
    ) -> NpIntMatrix:
        return (
            lf.select(self._to)
            .with_columns(all_terms)
            .filter(pl.col("terms").is_in(_from))
            .drop("terms")
            .collect()
            .to_numpy()
        )

    def _split_anno(self) -> list:
        """
        Splits annotation matrix into chunks row-wise to reduce computational overhead
        for matrix multiplication. Each chunk will have at most 1000 entries.
        """
        nchunks = self.anno.ids.height // 500
        if nchunks == 0:
            nchunks = 1
        return np.array_split(self.anno.data.to_numpy().astype(np.float32), nchunks)

    def _vector_propagate(self):
        """
        Applies the dot product between an annotations matrix and familial adjacency
        matrices. Below is the computation:

            (samples x reference_terms) @ (reference_terms, propagated_terms)
                -> (samples x propagated_terms).

        This is done once for ancestors and once for descendants. Then for each sample,
        if a term is is not an ancestor or descendant of that sample, then the sample is
        given a negative label for that term.

        """
        split = self._split_anno()
        propagated = self.propagator.multiprocess_propagate(
            self.anno, split, self.family, self._relatives
        )

        neg_mask = (propagated["ancestors"] == 0) & (propagated["descendants"] == 0)

        self.anno = propagated["ancestors"]
        self.anno[neg_mask] = -1
        self.anno = np.where(self.anno >= 1, 1, self.anno)

    def _propagate_up(self):
        split = self._split_anno()
        propagated = self.propagator.multiprocess_propagate(
            self.anno.n_indices, split, self.family["ancestors"], "ancestors"
        )
        propagated = np.where(propagated >= 1, 1, propagated)

        propagated = pl.DataFrame(propagated, schema=list(self.family["ids"]))

        return propagated, self.anno.ids

    def _propagate_down(self):
        split = self._split_anno()
        propagated = self.propagator.multiprocess_propagate(
            self.anno.n_indices, split, self.family["descendants"], "descendants"
        )
        propagated = np.where(propagated >= 1, 1, propagated)

        propagated = pl.DataFrame(propagated, schema=list(self.family["ids"]))

        return propagated, self.anno.ids
