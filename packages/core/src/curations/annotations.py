"""
Class for storing and mutating annotation collections.

Author: Parker Hicks
Date: 2025-04-14

Last updated: 2025-09-01 by Parker Hicks
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import polars as pl

from curations.index import Ids
from curations.base import BaseCuration
from curations.labels import Labels
from curations.propagator import Propagator
from ontology.graph import Graph
from util.alltypes import FilePath, IdArray, NpIdArray
from util.helpers import flatten_list
from util.io import load_txt
from util.supported import onto_relations, ontologies


class Annotations(BaseCuration):
    """
    Class to store and mutate annotations of samples to various attributes
    like tissues, dieases, sexes, ages, etc.

    Attributes
    ----------
    data: (pl.DataFrame)
        Polars DataFrame with columns `index`, `groups` and columns for each
        attribute entity for each index (e.g. male or female, tissues, diseases, etc).

    disease: (bool)
        Indicates if the annotations are disease based. Used to account for control samples
        when converting annotations to labels.

    index_col: (IdArray)
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

    def propagate_controls(
        self,
        ctrl_id: pl.DataFrame,
        terms: IdArray,
        labels: pl.DataFrame,
        group_col: str,
    ):
        """
        Propagates samples annotated as controls to diseases that other
        samples within the same study are annotated to

        Parameters
        ----------
        ctrl_id: pl.DataFrame
           Index to group mapping for control samples.

        terms: IdArray
            All terms for which there are labels.

        labels: pl.DataFrame
            DataFrame with terms as columns, samples as rows, and -1, 0, +1
            labels.

        group_col: str
            Name of the groups column in ctrl_id.

        Returns
        -------
        A Labels object with propagated controls.

        """
        labels_mat = labels.to_numpy()
        ctrl_dfs = []
        for group in ctrl_id[group_col].unique():
            _controls = ctrl_id.filter(pl.col(group_col) == group)[
                self.index_col
            ].to_list()
            ctrl_anno = np.zeros((len(_controls), len(terms)), dtype=np.int32)

            _anno = labels_mat[np.where(np.array(self.groups) == group)[0], :]
            positive_annos = np.any(_anno == 1, axis=0)

            ctrl_anno[:, positive_annos] = 2

            ctrl_dfs.append(
                ctrl_id.filter(pl.col("index").is_in(_controls))
                .select("index")
                .hstack(pl.DataFrame(ctrl_anno, schema=terms))
            )

        return pl.concat(ctrl_dfs).lazy()

    def to_labels(
        self,
        reference: str,
        to: Literal["system_descendants", "all"] = "all",
        ctrl_col: str = "MONDO:0000000",
        group_col: str = "group",
    ) -> Labels:
        """Convert annotations to propagated labels.

        Assigns propagated labels to terms given their annotations.

        Parameters
        ----
        reference: str
            The name of an ontology to reference for annotation propagation.
        terms: IdArray | str
            Array of terms to generate labels for, or "union"/"all".
        to: Optional[str]
            Target for propagation (e.g., "system_descendants").
        ctrl_col: str
            Column name for control samples.

        Returns
        -------
        A Labels curation object with propagated -1, 0, +1 labels.

        """
        # extract controls if they exist
        ctrl_ids = self._prepare_control_data(ctrl_col)

        # get terms to propagate to
        graph = Graph.from_obo(ontologies(reference), ontology=reference)
        terms_in_graph = self._prepare_terms(graph)
        propagate_to = self._prepare_targets(graph, reference, to)

        # propagate
        labels = self._propagate_annotations(reference, terms_in_graph, propagate_to)

        # handle controls
        if self.controls and ctrl_ids is not None and not self.collapsed:
            print(f"Propagating {len(ctrl_ids)} controls.")
            ctrl_labels = self.propagate_controls(
                ctrl_ids, list(propagate_to), labels, group_col
            )
            labels = pl.concat([self._ids.data, labels], how="horizontal").lazy()
            return Labels.from_df(
                labels.update(ctrl_labels, on=self.index_col, how="full")
            )

        # combine IDs and labels
        combined_labels = pl.concat([self._ids.data, labels], how="horizontal")
        return Labels(combined_labels)

    def to_numpy(self):
        """Returns the annotation data as a numpy array."""
        return self.data.to_numpy()

    def to_parquet(self, file: FilePath, **kwargs):
        """Save annotations to parquet file."""
        self._ids.data.hstack(self.data).write_parquet(file, **kwargs)

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
            "group_cols": tuple(new_groups),
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

    def _get_union_terms(self, graph: Graph):
        """Get intersection of graph nodes and current annotation columns."""
        _, _, in_graph = np.intersect1d(graph.nodes, self.entities, return_indices=True)
        return np.array(self.entities)[in_graph]

    def _filter_to_system_descendants(self, graph: Graph, reference: str) -> NpIdArray:
        """Filter terms to system descendants if requested."""
        print(f"Propagating to {reference} system descendants.")
        systems = load_txt(onto_relations(reference, "systems"))
        _to = list(set(flatten_list(list(graph.descendants_from(systems).values()))))
        _, _, in_systems = np.intersect1d(_to, self.entities, return_indices=True)
        return np.array(self.entities)[in_systems]

    def _prepare_control_data(self, ctrl_col: str):
        """Extract control data and update main data."""
        if ctrl_col not in self.entities:
            return None

        self.controls = True
        # combine ids and data to filter controls
        combined = pl.concat([self._ids.data, self.data], how="horizontal")
        ctrl_ids = combined.filter(pl.col(ctrl_col) == 1).select(self._ids.data.columns)

        # Remove control column from main data
        self.data = self.data.drop(ctrl_col)

        return ctrl_ids

    def _prepare_terms(self, graph: Graph) -> NpIdArray:
        """Prepare and validate terms for propagation."""
        return self._get_union_terms(graph)

    def _propagate_annotations(self, reference: str, _from: IdArray, _to: IdArray):
        """Perform the actual annotation propagation."""
        print(
            f"Propagating {self.n_indices} annotations to {len(_from)} terms to {len(_to)} terms."
        )
        # select terms to propagate to
        self.data = self.data.select(_from)

        propagator = Propagator(reference, self.data.to_numpy(), _from, _to)
        return propagator.propagate()

    def _prepare_targets(self, graph: Graph, reference: str, to: str) -> list[str]:
        """Returns system descendants or all terms in the graph to propagate to."""
        if isinstance(to, str) and to == "system_descendants":
            return self._filter_to_system_descendants(graph, reference)

        if isinstance(to, str) and to == "all":
            return graph.nodes

        supported: list[str] = ["system_descendants", "all"]
        raise ValueError(f"Expected to in {supported}, got {to}.")

    @classmethod
    def from_df(
        cls,
        df: pl.DataFrame,
        index_col: str,
        group_cols: tuple[str, str] = ("group", "platform"),
        **kwargs,
    ):
        """Creates an Annotations object from a combined DataFrame."""
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
