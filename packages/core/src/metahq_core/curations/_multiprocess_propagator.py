"""
Helper class to facilitate propagation of annotations by chunks.

Author: Parker Hicks
Date: 2025-09-26

Last updated: 2025-09-26
"""

from __future__ import annotations

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import polars as pl

from metahq_core.util.alltypes import NpIntMatrix
from metahq_core.util.progress import progress_bar


class MultiprocessPropagator:
    """Exists to allow multiprocessing within the Propagator class."""

    def __init__(self, verbose):
        self.verbose: bool = verbose

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
        n_processes: int | None = None,
        desc="Propagating",
    ):
        """Multiprocessing propagation"""
        if n_processes is None:
            n_processes = mp.cpu_count() - 1

        final_shape = (
            n_indices,
            family.shape[1],
        )

        propagated = np.empty(final_shape, dtype=np.int32)
        args_list = [(i, chunk, family) for i, chunk in enumerate(split)]

        with ProcessPoolExecutor(max_workers=n_processes) as executor:
            if self.verbose:
                results = self._execute_verbose(executor, args_list, desc)
            else:
                results = self._execute_silent(executor, args_list)

        # combine
        start = 0
        for _, result in sorted(results, key=lambda x: x[0]):
            nsamples = result.shape[0]
            end = start + nsamples
            propagated[start:end] = result
            start = end

        return propagated

    def _execute_silent(self, executor: ProcessPoolExecutor, args_list: list) -> list:
        futures = {
            executor.submit(self._process_chunk, args): args for args in args_list
        }
        return [future.result() for future in as_completed(futures)]

    def _execute_verbose(
        self, executor: ProcessPoolExecutor, args_list: list, desc: str
    ) -> list:
        with progress_bar() as progress:
            task = progress.add_task(desc, total=len(args_list))

            futures = {
                executor.submit(self._process_chunk, args): args for args in args_list
            }
            results = []
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                progress.update(task, description=desc, advance=1)
                progress.refresh()

        return results


def process_group_controls(args):
    """Performs the propagation for a subset of groups."""
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
    controls: pl.DataFrame,
    labels: NpIntMatrix,
    label_ids: pl.DataFrame,
    diseases: list[str],
    index_col: str,
    group_col: str,
    n_processes: int | None = None,
    verbose: bool = True,
):
    """
    Propagate control samples to diseases in which other samples within
    the same study as the control samples are annotated to.


    controls: pl.DataFrame
        DataFrame of index and group IDs of indices annotated as controls
        in the original annotations matrix. Note the index and group columns
        must match those of the label IDs.

    labels: NpIntMatrix
        Propagated labels with -1, 0, +1 labels to disease terms.

    """
    if n_processes is None:
        n_processes = mp.cpu_count() - 1

    groups = controls[group_col].unique().to_list()
    nchunks = len(groups) // 50
    if nchunks == 0:
        nchunks = 1

    split = np.array_split(groups, nchunks)

    args_list = [
        (controls, _split, labels, label_ids, diseases, index_col, group_col)
        for _split in split
    ]

    with ProcessPoolExecutor(max_workers=n_processes) as executor:
        futures = {
            executor.submit(process_group_controls, args): args for args in args_list
        }

        if verbose:
            with progress_bar() as progress:
                task = progress.add_task("Propagating controls", total=len(args_list))

                futures = {
                    executor.submit(process_group_controls, args): args
                    for args in args_list
                }
                results = []
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    progress.update(task, description="Propagating controls", advance=1)
                    progress.refresh()

        else:
            results = [future.result() for future in as_completed(futures)]

    return pl.concat(results)
