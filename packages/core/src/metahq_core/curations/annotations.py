"""
Class for storing and mutating annotation collections.

Author: Parker Hicks
Date: 2025-04-14

Last updated: 2026-02-02 by Parker Hicks
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.curations.annotation_converter import AnnotationsConverter
from metahq_core.curations.base import BaseCuration
from metahq_core.curations.index import Ids
from metahq_core.curations.labels import Labels
from metahq_core.export.annotations import AnnotationsExporter
from metahq_core.logger import setup_logger
from metahq_core.util.supported import get_default_log_dir

if TYPE_CHECKING:
    import logging


class Annotations(BaseCuration):
    """
    Class to store and mutate annotations of samples to various attributes
    like tissues, dieases, sexes, ages, etc.

    Attributes:
        data (pl.DataFrame):
            Polars DataFrame with index and group ID columns and columns for each
                attribute entity for each index (e.g. male or female, tissues, diseases, etc).

        disease (bool):
            Indicates if the annotations are disease based. Used to account for control samples
                when converting annotations to labels.

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
        group_cols: tuple[str, ...] = ("series", "platform"),
        collapsed: bool = False,
        logger=None,
        loglevel=20,
        logdir=get_default_log_dir(),
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

    def add_ids(self, new: pl.DataFrame) -> Annotations:
        """Append new group ID columns to the IDs of an Annotations object. The new
        IDs must have a matching index.

        Arguments:
            new (pl.DataFrame):
                A DataFrame of additional IDs to join with the current index column of `data`.
                    Must have a matching index column as the original `data`.

        Returns:
            A new Annotations object including the new ID columns.
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

    def collapse(self, on: str, inplace: bool = True):
        """Collapses annotations on the specified grouping column.

        Arguments:
            on (str):
                The column to collapse on. This should be one of the columns in `group_cols`.
            inplace (bool):
                If True, updates this object and returns self. Otherwise, returns new object.
        """
        params = self._collapse(on)

        if inplace:
            self.data = params["data"]
            self._ids = Ids.from_dataframe(params["ids"], params["index_col"])
            self.index_col = params["index_col"]
            self.group_cols = params["group_cols"]
            self.collapsed = params["collapsed"]
            return self

        return self.__class__(**params)

    def drop(self, *args, **kwargs) -> Annotations:
        """Wrapper for polars drop. Drops any of the term columns.
        ID columns are not dropped through this method.
        """
        return self.__class__(
            data=self.data.drop(*args, **kwargs),
            ids=self.ids,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
            logger=self.log,
            verbose=self.verbose,
        )

    def filter(self, condition: pl.Expr) -> Annotations:
        """Filter both data and ids simultaneously using a mask.

        Arguments:
            condition (pl.Expr):
                Polars expression for filtering columns.

        Examples:
            >>> from metahq_core.curations.annotations import Annotations
            >>> anno = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, 0, 0],
                    'UBERON:0002113': [0, 1, 0],
                    'UBERON:0000955': [0, 0, 1],
                }
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.filter(pl.col("UBERON:0000948") == 1)
            ┌────────┬────────┬────────────────┬────────────────┬────────────────┐
            │ sample ┆ series ┆ UBERON:0000948 ┆ UBERON:0002113 ┆ UBERON:0000955 │
            │ ---    ┆ ---    ┆ ---            ┆ ---            ┆ ---            │
            │ str    ┆ str    ┆ i32            ┆ i32            ┆ i32            │
            ╞════════╪════════╪════════════════╪════════════════╪════════════════╡
            │ GSM1   ┆ GSE1   ┆ 1              ┆ 0              ┆ 0              │
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
        attribute: str,
        level: str,
        metadata: str | None = None,
    ):
        """Save the annotations curation.

        Arguments:
            outfile (str | Path):
                Path to outfile.json.

            fmt (Literal["json", "parquet", "csv", "tsv"]):
                File format to save to.

            attribute (str):
                A supported MetaHQ annotated attribute.

            level (str):
                An index level supported by MetaHQ.

            metadata (bool):
                If True, will add index titles to each entry.
        """
        AnnotationsExporter(
            attribute, level, logger=self.log, verbose=self.verbose
        ).save(self, fmt, outfile, metadata)

    def sort_columns(self):
        """Sorts term columns.

        Examples:
            >>> from metahq_core.curations.annotations import Annotations
            >>> anno = {
                    'sample': ['GSM1', 'GSM2', 'GSM3'],
                    'series': ['GSE1', 'GSE1', 'GSE2'],
                    'UBERON:0000948': [1, 0, 0],
                    'UBERON:0002113': [0, 1, 0],
                    'UBERON:0000955': [0, 0, 1],
                }
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.sort_columns()
            ┌────────┬────────┬────────────────┬────────────────┬────────────────┐
            │ series ┆ sample ┆ UBERON:0000948 ┆ UBERON:0000955 ┆ UBERON:0002113 │
            │ ---    ┆ ---    ┆ ---            ┆ ---            ┆ ---            │
            │ str    ┆ str    ┆ i32            ┆ i32            ┆ i32            │
            ╞════════╪════════╪════════════════╪════════════════╪════════════════╡
            │ GSE1   ┆ GSM1   ┆ 1              ┆ 0              ┆ 0              │
            │ GSE1   ┆ GSM2   ┆ 0              ┆ 0              ┆ 1              │
            │ GSE2   ┆ GSM3   ┆ 0              ┆ 1              ┆ 0              │
            └────────┴────────┴────────────────┴────────────────┴────────────────┘
        """
        return self.__class__(
            data=self.data.select(sorted(self.data.columns)),
            ids=self.ids,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
            logger=self.log,
            verbose=self.verbose,
        )

    def propagate(
        self,
        to_terms: list[str],
        ontology: str,
        mode: Literal[0, 1],
        control_col: str = "MONDO:0000000",
    ) -> Labels | Annotations:
        """Convert annotations to propagated labels.

        Assigns propagated labels to terms given their annotations.

        Arguments:
            to_terms (list[str]):
                Array of terms to generate labels for, or "union"/"all".

            ontology (str):
                The name of an ontology to reference for annotation propagation.

            mode (Literal[0, 1]):
                Mode of propagation.

                    If mode is 0, this will propagate any positive annotations
                    from any descendants of the to_terms up to the to_terms.

                    If mode 1, this will convert annotations to -1, 0, +1 labels
                    where for a particular term, if an index is annotated to that term or
                    any of its descendants, it recieves a +1 label. If it is annotated to an
                    ancestor of that term, it receives a 0 (unsure) label. If it is not annotated
                    to an ancestor or a descendant of that term, it recieves a -1 label.
                    Any indices annotated to the control column are assigned a label of 2 for any
                    terms that other indices within the same group are positively labeled to.

            control_col (str):
                Column name for control annotations.

        Returns:
            A Labels curation object with propagated -1, 0, +1 labels (and 2 if controls are
            present). Any entries in `index_col` that have a 0 annotation/label across all
            entity columns are dropped.

        Examples:

            With `mode=0`:

            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.propagate(to_terms=["UBERON:0000948"], ontology="uberon", mode=0)
            ┌────────┬────────┬────────────────┐
            │ sample ┆ series ┆ UBERON:0000948 │
            │ ---    ┆ ---    ┆ ---            │
            │ str    ┆ str    ┆ i32            │
            ╞════════╪════════╪════════════════╡
            │ GSM1   ┆ GSE1   ┆ 1              │
            │ GSM2   ┆ GSE1   ┆ 1              │
            └────────┴────────┴────────────────┘

            With `mode=1`:

            >>> anno.propagate(to_terms=["UBERON:0000948"], ontology="uberon", mode=1)
            ┌────────┬────────┬────────────────┐
            │ sample ┆ series ┆ UBERON:0000948 │
            │ ---    ┆ ---    ┆ ---            │
            │ str    ┆ str    ┆ i32            │
            ╞════════╪════════╪════════════════╡
            │ GSM1   ┆ GSE1   ┆ 1              │
            │ GSM2   ┆ GSE1   ┆ 1              │
            │ GSM3   ┆ GSE2   ┆ -1             │
            └────────┴────────┴────────────────┘
        """
        converter = AnnotationsConverter(
            self,
            to_terms,
            ontology,
            control_col=control_col,
            logger=self.log,
            verbose=self.verbose,
        )

        if mode == 0:
            propagated, ids = converter.propagate_up()
            return self.__class__(
                data=propagated,
                ids=ids,
                index_col=self.index_col,
                group_cols=self.group_cols,
                logger=self.log,
                verbose=self.verbose,
            )

        if mode == 1:
            return converter.to_labels()

        msg = ("Mode %s not available.", mode)
        if self.verbose:
            self.log.error(msg)

        raise ValueError(msg)

    def select(self, *args, **kwargs) -> Annotations:
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

    def slice(self, offset: int, length: int | None = None) -> Annotations:
        """Slice both data and ids simultaneously using `polars` slice.

        Arguments:
            offset (int):
                Index position to begin the slice.

            length (int | None):
                Number of indices past `offset` to slice out.

        Returns:
            Sliced Annotations object as a subset of the original Annotations.
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

    def _collapse(self, on: str):
        """Collapses index-level annotations to group-level. Helper function
        for `collapse`.
        """
        index_anno = self.data.with_columns(self.ids[on])
        agg_anno = index_anno.group_by(on).agg(pl.col("*").sum()).sort(on)
        new_ids = self._collapse_ids(on, keep=agg_anno[on].to_list())

        agg_anno = agg_anno.drop(on)
        for col in agg_anno.columns:
            if col in self.group_cols:
                continue

            agg_anno = agg_anno.with_columns(
                pl.when(pl.col(col) > 0).then(1).otherwise(0).alias(col)
            )

        new_groups = list(self.group_cols)
        new_groups.remove(on)

        params = {
            "data": agg_anno,
            "ids": new_ids,
            "index_col": on,
            "group_cols": new_groups,
            "collapsed": True,
        }
        return params

    def _collapse_ids(self, on: str, keep: list[str]):
        """Group IDs to keep in the new collapsed frame. Helper function
        for `collapse`.
        """
        return (
            self.ids.drop(self.index_col)
            .unique()
            .filter(pl.col(on).is_in(keep))
            .sort(on)
        )

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        index_col: str,
        group_cols: tuple[str, ...] | list[str],
        **kwargs,
    ) -> Annotations:
        """Creates an Annotations object from a combined DataFrame.

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
            An Annotations object constructed from `df`.

        Examples:
            >>> from metahq_core.curations.annotations import Annotations
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            ┌────────┬────────┬────────────────┬────────────────┬────────────────┬────────────────┐
            │ sample ┆ series ┆ UBERON:0000948 ┆ UBERON:0002349 ┆ UBERON:0002113 ┆ UBERON:0000955 │
            │ ---    ┆ ---    ┆ ---            ┆ ---            ┆ ---            ┆ ---            │
            │ str    ┆ str    ┆ i64            ┆ i64            ┆ i64            ┆ i64            │
            ╞════════╪════════╪════════════════╪════════════════╪════════════════╪════════════════╡
            │ GSM1   ┆ GSE1   ┆ 1              ┆ 1              ┆ 0              ┆ 0              │
            │ GSM2   ┆ GSE1   ┆ 0              ┆ 1              ┆ 0              ┆ 0              │
            │ GSM3   ┆ GSE2   ┆ 0              ┆ 0              ┆ 0              ┆ 1              │
            └────────┴────────┴────────────────┴────────────────┴────────────────┴────────────────┘
        """
        group_cols = tuple(group_cols)
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
        """Returns term names of the Annotations frame.

        Examples:
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.entities
            ['UBERON:0000955', 'UBERON:0002349', 'UBERON:0000948', 'UBERON:0002113']
        """
        return list(set(self.data.columns) - set(self.ids.columns))

    @property
    def groups(self) -> list[str]:
        """Returns the groups column of the Annotations curation.


        Examples:
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.groups
            ['GSE1', 'GSE1', 'GSE2']

        """
        return self.ids["series"].to_list()

    @property
    def ids(self) -> pl.DataFrame:
        """Return the IDs dataframe.


        Examples:
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.ids
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
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.index
            ['GSM1', 'GSM2', 'GSM3']
        """
        return self._ids.index.to_list()

    @property
    def n_indices(self) -> int:
        """Returns number of indices.

        Examples:
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.n_indices
            3
        """
        return self.data.height

    @property
    def n_entities(self) -> int:
        """Returns number of entities.

        Examples:
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.n_entities
            4
        """
        return len(self.entities)

    @property
    def unique_groups(self) -> list[str]:
        """Returns unique groups.

        Examples:
            >>> anno = pl.DataFrame(
                    {
                        "series": ["GSE1", "GSE1", "GSE2"],
                        "sample": ["GSM1", "GSM2", "GSM3"],
                        "UBERON:0000948": [1, 0, 0],
                        "UBERON:0002349": [1, 1, 0],
                        "UBERON:0002113": [0, 0, 0],
                        "UBERON:0000955": [0, 0, 1],
                    }
                )
            >>> anno = Annotations.from_df(anno, index_col="sample", group_cols=["series"])
            >>> anno.unique_groups
            ['GSE2', 'GSE1']
        """
        return list(set(self.groups))

    def __repr__(self):
        return repr(self._ids.data.hstack(self.data))
