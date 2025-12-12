"""
Class for mutating and operating on sets of labels.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-11-27 by Parker Hicks
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.curations.base import BaseCuration
from metahq_core.curations.index import Ids
from metahq_core.export.labels import LabelsExporter
from metahq_core.logger import setup_logger
from metahq_core.util.alltypes import NpIntMatrix

if TYPE_CHECKING:
    import logging


# TODO: Add method to remove redundant terms
class Labels(BaseCuration):
    """Class for storing and mutating labels.

    Currently supports -1, 0, +1 labels.

    Attributes:
        data (pl.DataFrame):
            Polars DataFrame with columns `index`, `groups` and columns for each
            attribute entity for each index (e.g. male or female, tissues, diseases, etc).

        index_col (str):
            Name of the column of data that contains the index IDs.

        group_cols (tuple[str, ...]):
            Names of columns of data that contain an ID for each index indicating if it belongs
            to a particular group (e.g. dataset, sex, platform, etc.).

        collapsed (bool):
            Indicates if the annotations have already been collapsed.
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
        """Append new group ID columns to the IDs of a Labels object. The new
        IDs must have a matching index.

        Arguments:
            new (pl.DataFrame):
                A DataFrame of additional IDs to join with the current index column of `data`.
                    Must have a matching index column as the original `data`.

        Returns:
            A new Labels object including the new ID columns.
        """
        new_ids = new.join(
            self.ids, on=self.index_col, how="inner", maintain_order="right"
        )
        new_groups = tuple(col for col in new_ids.columns if col != self.index_col)
        assert new_ids.height == self.ids.height, "SRA IDs height mismatch."
        assert (
            new_ids[self.index_col].to_list() == self.index
        ), "Index order does not match."

        return self.__class__(
            self.data, new_ids, index_col=self.index_col, group_cols=new_groups
        )

    def drop(self, *args, **kwargs):
        """Wrapper for polars drop. Drops any of the term columns.
        ID columns are not dropped through this method.
        """
        self.data = self.data.drop(*args, **kwargs)

    def filter(self, condition: pl.Expr) -> Labels:
        """Filter both data and ids simultaneously using a mask.

        Arguments:
            condition (pl.Expr):
                Polars expression for filtering columns.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col="sample", group_cols=["series"])
            >>> labels.filter(pl.col("UBERON:0000948") == 1)
            ┌────────┬────────┬────────────────┬────────────────┬────────────────┐
            │ sample ┆ series ┆ UBERON:0000948 ┆ UBERON:0002113 ┆ UBERON:0000955 │
            │ ---    ┆ ---    ┆ ---            ┆ ---            ┆ ---            │
            │ str    ┆ str    ┆ i32            ┆ i32            ┆ i32            │
            ╞════════╪════════╪════════════════╪════════════════╪════════════════╡
            │ GSM1   ┆ GSE1   ┆ 1              ┆ -1             ┆ -1             │
            └────────┴────────┴────────────────┴────────────────┴────────────────┘
        """
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
        outfile: str | Path,
        fmt: Literal["json", "parquet", "csv", "tsv"],
        metadata: str | None = None,
    ):
        """Save the labels curation.

        Arguments:
            outfile (str | Path):
                Path to outfile.json.

            fmt (Literal["json", "parquet", "csv", "tsv"]):
                File format to save to.

            metadata (str | None):
                Metadata fields to inlcude formatted as a comma
                delimited string.

        Examples:

            If `metadata` is None, will only save the index column
            with the remaining labels.

            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.save('/path/to/out.parquet', fmt="parquet")

        """
        LabelsExporter(logger=self.log, verbose=self.verbose).save(
            self, fmt, outfile, metadata
        )

    def select(self, *args, **kwargs) -> Labels:
        """Select label entity columns while maintaining ids."""
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
        """Slice both data and ids simultaneously using `polars` slice.

        Arguments:
            offset (int):
                Index position to begin the slice.

            length (int | None):
                Number of indices past `offset` to slice out.

        Returns:
            Sliced Labels object as a subset of the original Labels.
        """
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

    def to_numpy(self) -> NpIntMatrix:
        """Wrapper for polars `to_numpy`."""
        return LabelsExporter().to_numpy(self)

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        index_col: str,
        group_cols: tuple[str, ...] | list[str],
        **kwargs,
    ) -> Labels:
        """Creates a Labels object from a combined DataFrame.

        Attributes:
            df (pl.DataFrame):
                Polars DataFrame with index and group ID columns and columns for each
                    attribute entity for each index (e.g. male or female, tissues, diseases, etc).

            index_col (str):
                Name of the column of data that contains the index IDs.

            group_cols (tuple[str, ...]):
                Names of columns of data that contain an ID for each index indicating if it belongs
                    to a particular group (e.g. dataset, sex, platform, etc.).

        Returns:
            A Labels object constructed from `df`.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            ┌────────┬────────┬────────────────┬────────────────┬────────────────┐
            │ sample ┆ series ┆ UBERON:0000948 ┆ UBERON:0002113 ┆ UBERON:0000955 │
            │ ---    ┆ ---    ┆ ---            ┆ ---            ┆ ---            │
            │ str    ┆ str    ┆ i64            ┆ i64            ┆ i64            │
            ╞════════╪════════╪════════════════╪════════════════╪════════════════╡
            │ GSM1   ┆ GSE1   ┆ 1              ┆ -1             ┆ -1             │
            │ GSM2   ┆ GSE1   ┆ -1             ┆ -1             ┆ -1             │
            │ GSM3   ┆ GSE2   ┆ -1             ┆ -1             ┆ 1              │
            └────────┴────────┴────────────────┴────────────────┴────────────────┘
        """
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
        """Returns column names of the Labels frame.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.entities
            ['UBERON:0000948', 'UBERON:0002113', 'UBERON:0000955']
        """
        return self.data.columns

    @property
    def groups(self) -> list[str]:
        """Returns the groups column of the Labels curation.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.groups
            ['GSE1', 'GSE1', 'GSE2']
        """
        return self.ids["group"].to_list()

    @property
    def ids(self) -> pl.DataFrame:
        """Return the IDs dataframe.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.ids
            ┌────────┬────────┐
            │ sample ┆ series │
            │ ---    ┆ ---    │
            │ str    ┆ str    │
            ╞════════╪════════╡
            │ GSM1   ┆ GSE1   │
            │ GSM2   ┆ GSE1   │
            │ GSM3   ┆ GSE2   │
            └────────┴────────┘
        """
        return self._ids.data

    @property
    def index(self) -> list[str]:
        """Return the index column as a list.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.index
            ['GSM1', 'GSM2', 'GSM3']
        """
        return self._ids.index.to_list()

    @property
    def n_indices(self) -> int:
        """Returns number of indices.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.n_indices
            3
        """
        return self.data.height

    @property
    def n_entities(self) -> int:
        """Returns number of entities.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                    'UBERON:0002107': [-1, -1, -1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.n_entities
            4
        """
        return len(self.entities)

    @property
    def unique_groups(self) -> list[str]:
        """Returns unique groups.

        Examples:
            >>> from metahq_core.curations.labels import Labels
            >>> labels = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, -1, -1],
                    'UBERON:0002113': [-1, 1, -1],
                    'UBERON:0000955': [-1, -1, 1],
                }
            >>> labels = Labels.from_df(anno, index_col='sample', group_cols=['series'])
            >>> labels.unqiue_groups
            ['GSE1', 'GSE2']
        """
        return list(set(self.groups))

    def __repr__(self):
        return repr(self._ids.data.hstack(self.data))
