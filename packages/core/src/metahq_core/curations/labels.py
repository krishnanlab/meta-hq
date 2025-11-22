"""
Class for mutating and operating on sets of labels.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-11-21 by Parker Hicks
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import numpy as np
import polars as pl

from metahq_core.curations.base import BaseCuration
from metahq_core.curations.index import Ids
from metahq_core.export.labels import LabelsExporter
from metahq_core.logger import setup_logger
from metahq_core.util.alltypes import FilePath, NpIntMatrix

if TYPE_CHECKING:
    import logging


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
        logger=None,
        loglevel=20,
        logdir=Path("."),
        verbose=True,
    ):
        self.data = data
        self.index_col = index_col
        self.group_cols = group_cols
        self._ids = Ids.from_dataframe(ids, index_col)
        self.collapsed = collapsed
        self.controls: bool = False

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def add_ids(self, new: pl.DataFrame) -> Labels:
        """
        Append new group ID columns to the IDs of a Labels object. The new
        IDs must have a matching index.
        """
        new_ids = new.join(
            self.ids, on=self.index_col, how="inner", maintain_order="right"
        )
        new_groups = tuple([col for col in new_ids.columns if col != self.index_col])
        assert new_ids.height == self.ids.height, "SRA IDs height mismatch."
        assert (
            new_ids[self.index_col].to_list() == self.index
        ), "Index order does not match."

        return self.__class__(
            self.data, new_ids, index_col=self.index_col, group_cols=new_groups
        )

    def drop(self, *args, **kwargs):
        """Wrapper for polars drop."""
        self.data = self.data.drop(*args, **kwargs)

    def filter(self, condition: pl.Expr) -> Labels:
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
            logger=self.log,
            verbose=self.verbose,
        )

    def head(self, *args, **kwargs) -> str:
        """Wrapper for polars head function."""
        return repr(self.data.head(*args, **kwargs))

    def save(
        self,
        outfile: FilePath,
        fmt: Literal["json", "parquet", "csv", "tsv"],
        metadata: str | None = None,
    ):
        """
        Save labels curation to json. Keys are terms and values are
        positively annotated indices.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.json.

        metadata: bool
            If True, will add index titles to each entry.

        """
        LabelsExporter(logger=self.log, verbose=self.verbose).save(
            self, fmt, outfile, metadata
        )

    def select(self, *args, **kwargs) -> Labels:
        """Select annotation columns while maintaining ids."""
        selected_data = self.data.select(*args, **kwargs)

        return self.__class__(
            data=selected_data,
            ids=self._ids.data,  # keep all ID data
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
            logger=self.log,
            verbose=self.verbose,
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
            logger=self.log,
            verbose=self.verbose,
        )

    def subset_index(self, subset: list[str] | np.ndarray) -> Labels:
        """
        Selects rows of the expression frame whose sample IDs are in a specified
        subset. Note the returned order may not match.

        Parameters
        ----------
        subset: list[str] | np.ndarray
            Array-like of index IDs to select from the expression frame.

        Returns
        -------
        A new LazyExp object with the subset of index IDs in the frame.

        """
        _, _, mask = np.intersect1d(
            np.array(subset), np.array(self.index), return_indices=True
        )

        diff = abs(len(mask) != len(subset))
        if (diff != 0) and self.verbose:
            self.log.warning("%s indices not found in the frame.", diff)

        return self.__class__(
            data=self.data.with_row_index()
            .filter(pl.col("index").is_in(mask))
            .drop("index"),
            ids=self._ids.filter_by_mask(mask).data,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
            logger=self.log,
            verbose=self.verbose,
        )

    def to_numpy(self) -> NpIntMatrix:
        """Wrapper for polars `to_numpy`."""
        return LabelsExporter().to_numpy(self)

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        index_col: str,
        group_cols: tuple[str, ...] | list[str] = ("group", "platform"),
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
            group_cols=tuple(group_cols),
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
        return repr(self._ids.data.hstack(self.data))
