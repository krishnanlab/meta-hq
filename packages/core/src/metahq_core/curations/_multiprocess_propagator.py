"""
Helper class to facilitate propagation of annotations by chunks.

Author: Parker Hicks
Date: 2025-09-26

Last updated: 2025-11-08 by Parker Hicks
"""

from __future__ import annotations

import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING

import numpy as np

from metahq_core.logger import setup_logger
from metahq_core.util.alltypes import NpIntMatrix
from metahq_core.util.progress import progress_bar

if TYPE_CHECKING:
    import logging


class MultiprocessPropagator:
    """Exists to allow multiprocessing within the Propagator class."""

    def __init__(self, logger, verbose=True):
        self.log: logging.Logger = logger
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
        with progress_bar(padding="    ") as progress:
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
