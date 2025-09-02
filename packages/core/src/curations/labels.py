"""
Class for mutating and operating on sets of labels.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-09-01 by Parker Hicks
"""

import polars as pl

from curations.base import BaseCuration
from util.alltypes import FilePath, IdArray


# TODO: Add method to remove redundant terms
class Labels(BaseCuration):
    def __init__(self, data: pl.DataFrame):
        self.data = data

    def head(self, *args, **kwargs):
        """Wrapper for polars `head`."""
        return repr(self.data.head(*args, **kwargs))

    def filter(self, *args, **kwargs):
        """Wrapper for polars `filter`."""
        return Labels(self.data.filter(*args, **kwargs))

    def select(self, *args, **kwargs):
        """Wrapper for polars `select`."""
        return Labels(self.data.select(*args, **kwargs))

    def to_numpy(self):
        """Wrapper for polars `to_numpy`."""
        return self.data.to_numpy()

    def to_parquet(self, file: FilePath, ids: bool = True, **kwargs):
        """Writes labels with IDs to parquet file."""
        if not ids:
            ids_in_df = [col for col in ["index", "group"] if col in self.entities]
            for id_ in ids_in_df:
                self.data.drop(id_)
        self.data.write_parquet(file, **kwargs)

    @classmethod
    def from_df(cls, df: pl.DataFrame | pl.LazyFrame):
        """
        Create annotations object from a polars dataframe.

        Parameters
        ----------
        df: (pl.DataFrame)
            DataFrame with index, groups, and term columns.

        disease: (bool)
            Indicates if disease annotations.

        Returns
        -------
        An Annotations object.
        """
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        return Labels(df)

    @property
    def n_entities(self) -> int:
        """Returns number of entities."""
        return len(self.entities)

    @property
    def n_indices(self) -> int:
        """Returns number of indices."""
        return self.data.height

    @property
    def entities(self) -> IdArray:
        """Returns column names of the Annotations frame."""
        return self.data.columns

    def __repr__(self):
        return repr(self.data)
