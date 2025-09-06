"""
Class for performing annotation propagation.

Author: Parker Hicks
Date: 2025-04-23

Last updated: 2025-09-01 by Parker Hicks
"""

import multiprocessing as mp

import numpy as np
import polars as pl
from tqdm import tqdm

from metahq_core.util.alltypes import IdArray, NpIntMatrix, NpStringArray
from metahq_core.util.io import load_txt
from metahq_core.util.supported import onto_relations


class MultiprocessPropagator:
    """Class-based version to allow multiprocessing within Propagator."""

    @staticmethod
    def _process_chunk_static(args):
        """Static method worker function"""
        chunk_idx, chunk, family_relative, relatives_key = args
        result = np.einsum("ij,jk->ik", chunk, family_relative)
        return chunk_idx, result, relatives_key

    def multiprocess_propagate(
        self,
        anno,
        split: list,
        family: dict[str, np.ndarray],
        relatives: list,
        n_processes: int | None = None,
    ):
        """Multiprocessing propagation"""
        if n_processes is None:
            n_processes = mp.cpu_count()

        final_shape = (
            anno.shape[0],
            family["ancestors"].shape[1],
        )

        propagated = {
            "ancestors": np.empty(final_shape, dtype=np.int32),
            "descendants": np.empty(final_shape, dtype=np.int32),
        }

        for _relatives in relatives:

            args_list = [
                (i, chunk, family[_relatives], _relatives)
                for i, chunk in enumerate(split)
            ]

            with mp.Pool(processes=n_processes) as pool:
                results = list(
                    tqdm(
                        pool.imap(self._process_chunk_static, args_list),
                        total=len(args_list),
                        desc=f"Propagating {_relatives} with {n_processes} processes",
                    )
                )

            # Combine results
            start = 0
            for _, result, relatives_key in sorted(results, key=lambda x: x[0]):
                nsamples = result.shape[0]
                end = start + nsamples
                propagated[relatives_key][start:end] = result
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

    def __init__(self, ontology, anno, _from, _to):
        self.ontology: str = ontology
        self._from: IdArray = _from
        self._to: IdArray = _to
        self.anno: NpIntMatrix = anno
        self.family: dict[str, pl.DataFrame | NpStringArray] = {}
        self._relatives: list[str] = ["ancestors", "descendants"]
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

        Parameters
        ---------
        reference: (IdArray)
            Columns of self.anno to load in from the familial adjacency matrices.

        """
        self.family["ids"] = np.array(load_txt(onto_relations(self.ontology, "ids")))
        col_mask = np.isin(self.family["ids"], self._to)
        self.family["ids"] = self.family["ids"][col_mask]

        for item in self._relatives:
            self.family[item] = (
                pl.read_parquet(
                    onto_relations(self.ontology, item), columns=list(self._from)
                )
                .transpose()
                .to_numpy()[:, col_mask]
            )  # TODO: Look into if this is over complicated

    def _split_anno(self) -> list:
        """
        Splits annotation matrix into chunks row-wise to reduce computational overhead
        for matrix multiplication. Each chunk will have at most 1000 entries.
        """
        nchunks = self.anno.shape[0] // 500
        return np.array_split(self.anno.astype(np.float32), nchunks)

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
        self.anno = np.where(self.anno > 1, 1, self.anno)
