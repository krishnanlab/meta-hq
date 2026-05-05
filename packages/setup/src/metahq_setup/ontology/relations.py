from pathlib import Path

import polars as pl
from numpy.typing import NDArray


class RelationsMatrix:
    """Class to store and save ontology relations matrices.

    Attributes:
        matrix (NDArray):
            A terms x terms matrix storing binary relationships between term pairs.
                You may interpret it as the following: For any row, column pair, if the
                value is 1, then the term representing that particular row is an ancestor
                of the term representing that particular column. If the value is 0, then
                there is no relationship between the terms.

        terms (NDArray):
            An array representing the columns and rows of the matrix.
    """

    def __init__(self, matrix: NDArray, terms: NDArray):
        self.matrix = matrix
        self.terms = terms

    def save(self, outfile: Path):
        outdir = outfile.resolve().parents[0]
        if not outdir.exists():
            outdir.mkdir(exist_ok=True, parents=True)

        pl.LazyFrame(self.matrix, schema=list(self.terms), orient="row").sink_parquet(
            outfile, engine="streaming"
        )
