"""
Class for mutating and operating on sets of labels.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-09-02 by Parker Hicks
"""

from __future__ import annotations

import polars as pl

from curations.base import BaseCuration
from curations.index import Ids
from util.alltypes import FilePath, IdArray


# TODO: Add method to remove redundant terms
class Labels(BaseCuration):
    """
    Class for storing and mutating labels.

    Currently supports -1, 0, +1 labels.

    Attributes
    ---------
    data: pl.DataFrame
        Polars DataFrame with columns `index`, `groups` and columns for each
        attribute entity for each index (e.g. male or female, tissues, diseases, etc).

    disease: bool
        Indicates if the annotations are disease based. Used to account for control samples
        when converting annotations to labels.

    index_col: IdArray
        Name of the column of data that contains the index IDs.

    group_cols: tuple
        Names of columns of data that contain an ID for each index indicating if it belongs
        to a particular group (e.g. dataset, sex, platform, etc.).

    collapsed: bool
        Indicates if the annotations have already been collapsed.

    Methods
    -------
    drop()
        Wrapper for polars `drop`.

    filter()
        Wrapper for polars `filter`.

    head()
        Wrapper for polars `head`.

    select()
        Wrapper for polars `select`.

    slice()
        Wrapper for polars `slice`.

    Properties
    ---------
    entities: list[str]
        columns of the annotations frame of ontology terms.

    groups: list[str]
        Groups associated with each index of the annotations curation.
        Note that groups are not unique.

    ids: pl.DataFrame
        The frame of all IDs within the annotations curation.

    index
        The index IDs of the annotations frame.

    n_entities: int
        Number of unique entities.

    n_index: int
        Number of indices.

    unique_groups: list[str]
        Unique groups in the annotations curation.

    """
    def __init__(
        self,
        data: pl.DataFrame,
        ids: pl.DataFrame,
        index_col: str,
        group_cols: tuple[str, ...] = ("group", "platform"),
        collapsed: bool = False,
    ):
        self.data = data
        self.index_col = index_col
        self.group_cols = group_cols
        self._ids = Ids.from_dataframe(ids, index_col)
        self.collapsed = collapsed
        self.controls: bool = False

    def drop(self, *args, **kwargs):
        """Wrapper for polars drop."""
        self.data = self.data.drop(*args, **kwargs)

    def filter(self, condition: pl.Expr) -> Annotations:
        """Filter both data and ids simultaneously using a mask."""
        mask = self.data.select(condition.arg_true()).to_numpy().reshape(-1)

        filtered_data = (
            self.data.with_row_index().filter(pl.col("index").is_in(mask)).drop("index")
        )
        filtered_ids = self._ids.filter_by_mask(mask)

        return self.__class__(
            data=filtered_data,
            ids=filtered_ids.data,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
        )

    def head(self, *args, **kwargs):
        """Wrapper for polars head function."""
        return repr(self.data.head(*args, **kwargs))

    def select(self, *args, **kwargs) -> Annotations:
        """Select annotation columns while maintaining ids."""
        selected_data = self.data.select(*args, **kwargs)

        return self.__class__(
            data=selected_data,
            ids=self._ids.data,  # keep all ID data
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
        )

    def slice(self, offset: int, length: int | None = None) -> Labels:
        """Slice both data and ids simultaneously using polars slice."""
        sliced_data = self.data.slice(offset, length)
        sliced_ids_data = self._ids.data.slice(offset, length)

        return self.__class__(
            data=sliced_data,
            ids=sliced_ids_data,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
        )

    def to_numpy(self):
        """Wrapper for polars `to_numpy`."""
        return self.data.to_numpy()

    def to_parquet(self, file: FilePath, **kwargs):
        """Save annotations to parquet file."""
        self._ids.data.hstack(self.data).write_parquet(file, **kwargs)

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        index_col: str,
        group_cols: tuple[str, str] = ("group", "platform"),
        **kwargs,
    ) -> Labels:
        """Creates a Labels object from a combined DataFrame."""
        id_columns = [index_col] + list(group_cols)
        ids_data = df.select(id_columns)
        annotation_data = df.drop(id_columns)

        return cls(
            data=annotation_data,
            ids=ids_data,
            index_col=index_col,
            group_cols=group_cols,
            **kwargs,
        )
    
    @property
    def entities(self) -> list[str]:
        """Returns column names of the Annotations frame."""
        return self.data.columns

    @property
    def groups(self) -> list[str]:
        """Returns the groups column of the Annotations curation."""
        return self.ids["group"].to_list()

    @property
    def ids(self) -> pl.DataFrame:
        """Return the IDs dataframe."""
        return self._ids.data

    @property
    def index(self) -> list:
        """Return the index column as a list."""
        return self._ids.index.to_list()

    @property
    def n_indices(self) -> int:
        """Returns number of indices."""
        return self.data.height

    @property
    def n_entities(self) -> int:
        """Returns number of entities."""
        return len(self.entities)

    @property
    def unique_groups(self) -> list[str]:
        """Returns unique groups."""
        return list(set(self.groups))

    def __repr__(self):
        return repr(self.data)

