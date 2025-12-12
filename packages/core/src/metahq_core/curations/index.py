"""
Class to store and operate on indices for tabular data.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-11-28 by Parker Hicks
"""

from __future__ import annotations

import numpy as np
import polars as pl


class Ids:
    """A class to store and operate on ID columns for tabular data.
    Specifically made as an index for `polars.DataFrame` objects which
    lack index anchoring and tracking.

    Attributes:
        data (pl.DataFrame):
            DataFrame containing ID columns (index, group, platform, etc.)
        index_col (str):
            Name of the column that contains the primary index IDs.

    Examples:
        >>> from metahq_core.curations.index import Ids
        >>> ids = pl.DataFrame({
            "sample": ["GSM1", "GSM2", "GSM3"],
            "series": ["GSE1", "GSE1", "GSE2"],
            "platform": ["GPL10", "GPL10", "GPL23"],
            })
        >>> ids = ids.from_dataframe(ids, index_col="sample")
    """

    def __init__(self, data, index_col):
        self.data: pl.DataFrame = data
        self.index_col: str = index_col

    def filter_by_mask(self, mask: np.ndarray) -> Ids:
        """Filter the ids DataFrame using a boolean mask.

        Arguments:
            mask (np.ndarray):
                Array of indices to keep.

        Examples:
            >>> from metahq_core.curations.index import Ids
            >>> ids = pl.DataFrame({
                "sample": ["GSM1", "GSM2", "GSM3"],
                "series": ["GSE1", "GSE1", "GSE2"],
                "platform": ["GPL10", "GPL10", "GPL23"],
                })
            >>> ids = Ids.from_dataframe(ids, index_col="sample")
            >>> ids.filter_by_mask(np.array([1, 2])).data
            ┌────────┬────────┬──────────┐
            │ sample ┆ series ┆ platform │
            │ ---    ┆ ---    ┆ ---      │
            │ str    ┆ str    ┆ str      │
            ╞════════╪════════╪══════════╡
            │ GSM2   ┆ GSE1   ┆ GPL10    │
            │ GSM3   ┆ GSE2   ┆ GPL23    │
            └────────┴────────┴──────────┘
        """
        filtered_data = (
            self.data.with_row_index(name="tmp_idx")
            .filter(pl.col("tmp_idx").is_in(mask))
            .drop("tmp_idx")
        )
        return Ids(filtered_data, self.index_col)

    def lazy(self) -> pl.LazyFrame:
        """Wrapper for `polars.DataFrame.lazy()`.

        Returns:
            A `polars.LazyFrame` object of the `data` attribute.
        """
        return self.data.lazy()

    def to_numpy(self) -> np.ndarray:
        """Wrapper for `polars.DataFrame.to_numpy()`.

        Returns:
            The `data` attribute as a numpy ndarray.
        """
        return self.data.to_numpy()

    @classmethod
    def from_dataframe(cls, df: pl.DataFrame, index_col: str) -> Ids:
        """Creates an Ids object from a polars DataFrame.

        Arguments:
            df (pl.DataFrame):
                A `polars.DataFrame` object with at least one column.

            index_col (str):
                The name of the column in `df` that should be treated
                    as the index of the DataFrame.

        Returns:
            An initialized Ids object.

        Examples:
            >>> import polars as pl
            >>> from metahq_core.curations.index import Ids
            >>> ids = pl.DataFrame({
                    "sample": ["GSM1", "GSM2", "GSM3"],
                    "series": ["GSE1", "GSE1", "GSE2"],
                    "platform": ["GPL10", "GPL10", "GPL23"],
                })
            >>> Ids.from_dataframe(ids, index_col="sample")
        """
        return cls(df, index_col)

    def __getitem__(self, idx) -> Ids:
        """Slice the Ids frame with various indexing methods."""
        return Ids(self.data[idx], self.index_col)

    def __len__(self) -> int:
        """Return the number of rows."""
        return len(self.data)

    @property
    def columns(self) -> list[str]:
        """Returns columns of self.data.
        Wrapper for `polars.DataFrame.columns`.
        """
        return self.data.columns

    @property
    def index(self) -> pl.Series:
        """Get the index column as a Series.

        Examples:
            >>> import polars as pl
            >>> from metahq_core.curations.index import Ids
            >>> ids = pl.DataFrame({
                    "sample": ["GSM1", "GSM2", "GSM3"],
                    "series": ["GSE1", "GSE1", "GSE2"],
                    "platform": ["GPL10", "GPL10", "GPL23"],
                })
            >>> Ids.from_dataframe(ids, index_col="sample")
            shape: (3,)
            Series: 'sample' [str]
            [
                    "GSM1"
                    "GSM2"
                    "GSM3"
            ]
        """
        return self.data[self.index_col]
