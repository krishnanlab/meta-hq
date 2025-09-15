"""
Class for storing and mutating annotation collections.

Author: Parker Hicks
Date: 2025-04-14

Last updated: 2025-09-08 by Parker Hicks
"""

from __future__ import annotations

from typing import Literal

import polars as pl

from metahq_core.curations.annotation_converter import AnnotationsConverter
from metahq_core.curations.base import BaseCuration
from metahq_core.curations.index import Ids
from metahq_core.curations.labels import Labels
from metahq_core.export.annotations import AnnotationsExporter
from metahq_core.util.alltypes import FilePath


class Annotations(BaseCuration):
    """
    Class to store and mutate annotations of samples to various attributes
    like tissues, dieases, sexes, ages, etc.

    Attributes
    ----------
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
    collapse()
        Collapses index annotations to group annotations.

    drop()
        Wrapper for polars `drop`.

    filter()
        Wrapper for polars `filter`.

    from_df()
        Creates an Annotations object from a polars DataFrame or LazyFrame.

    head()
        Wrapper for polars `head`.

    propagate_controls()
        Propagates control samples to diseases that other samples in the same
        dataset are annotated to.

    select()
        Wrapper for polars `select`.

    slice()
        Wrapper for polars `slice`.

    to_labels()
        Propagates annotations to labels for an annotations matrix, given a reference
        ontology.

    to_numpy()
        Returns the annotations frame as a numpy 2D array.

    to_parquet()
        Saves the annotations frame and IDs to a .parquet file.

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

    def collapse(self, on: str, inplace: bool = True):
        """
        Collapses annotations on the specified grouping column.

        Args
        ----
        on: str
            The column to collapse on (should be one of the group_cols)
        inplace: bool
            If True, updates this object and returns self. If False, returns new object.
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
        """Wrapper for polars drop. Drops any of the term columns."""
        return self.__class__(
            data=self.data.drop(*args, **kwargs),
            ids=self.ids,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
        )

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
        Save annotations curation to json. Keys are terms and values are
        positively annotated indices.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.json.

        metadata: bool
            If True, will add index titles to each entry.

        """
        AnnotationsExporter().save(self, fmt, outfile, metadata)

    def sort_columns(self):
        """Sorts term columns."""
        return self.__class__(
            data=self.data.select(sorted(self.data.columns)),
            ids=self.ids,
            index_col=self.index_col,
            group_cols=self.group_cols,
            collapsed=self.collapsed,
        )

    def propagate(
        self,
        to_terms: list[str],
        ontology: str,
        mode: Literal[0, 1],
        control_col: str = "MONDO:0000000",
        group_col: str = "group",
    ) -> Labels:
        """Convert annotations to propagated labels.

        Assigns propagated labels to terms given their annotations.

        Parameters
        ----------
        to_terms: list[str]
            Array of terms to generate labels for, or "union"/"all".

        ontology: str
            The name of an ontology to reference for annotation propagation.

        mode: Literal[0, 1]
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

        control_col: str
            Column name for control annotations.

        group_col: str
            Column name of the group IDs. Used to assign control labels.

        Returns
        -------
        A Labels curation object with propagated -1, 0, +1 labels (and 2 if controls are present).

        """
        converter = AnnotationsConverter(
            self, to_terms, ontology, control_col=control_col
        )

        if mode == 0:
            return converter.propagate_up()

        if mode == 1:
            return converter.to_labels(groups=group_col)

        raise ValueError(f"Mode {mode} not available.")

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

    def slice(self, offset: int, length: int | None = None) -> Annotations:
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

    def _collapse(self, on: str):
        """Collapses index-level annotations to group-level."""
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
        """Group IDs to keep in the new collapsed frame."""
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
        group_cols: tuple[str, ...] | list[str] = ("group", "platform"),
        **kwargs,
    ) -> Annotations:
        """Creates an Annotations object from a combined DataFrame."""

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
        """Returns term names of the Annotations frame."""
        return list(set(self.data.columns) - set(self.ids.columns))

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
