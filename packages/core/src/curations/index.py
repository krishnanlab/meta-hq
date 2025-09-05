"""
Dataclass to store and operate on indices for tabular data.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-09-05 by Parker Hicks
"""

from __future__ import annotations

import numpy as np
import polars as pl


class Ids:
    """
    Dataclass to store and operate on ID columns for tabular data.
    Specifically made as an index for polars dataframes.

    Attributes
    ----------
    data: pl.DataFrame
        DataFrame containing ID columns (index, group, platform, etc.)
    index_col: str
        Name of the column that contains the primary index IDs

    Methods
    -------
    filter_by_mask()
        Filter rows of the frame by row indices.

    lazy()
        Wrapper for polars `lazy` conversion of a DataFrame to LazyFrame.

    to_numpy()
        Return IDs as numpy array.

    from_df()
        Create an Ids object from a polars DataFrame.

    Properties
    ----------
    index: pl.Series
        Returns the index column.

    """

    def __init__(self, data, index_col):
        self.data: pl.DataFrame = data
        self.index_col: str = index_col

    def filter_by_mask(self, mask: np.ndarray) -> Ids:
        """Filter the ids DataFrame using a boolean mask."""
        filtered_data = (
            self.data.with_row_index(name="tmp_idx")
            .filter(pl.col("tmp_idx").is_in(mask))
            .drop("tmp_idx")
        )
        return Ids(filtered_data, self.index_col)

    def lazy(self) -> pl.LazyFrame:
        """Returns the Ids as a polars LazyFrame."""
        return self.data.lazy()

    def to_numpy(self):
        """Returns the Ids as a numpy array."""
        return self.data.to_numpy()

    @classmethod
    def from_dataframe(cls, df: pl.DataFrame, index_col: str):
        """Creates an Ids object from a polars DataFrame."""
        return cls(df, index_col)

    def __getitem__(self, idx):
        """Slice the Ids frame with various indexing methods."""
        return Ids(self.data[idx], self.index_col)

    def __len__(self):
        """Return the number of rows."""
        return len(self.data)

    @property
    def index(self):
        """Get the index column as a Series."""
        return self.data[self.index_col]
