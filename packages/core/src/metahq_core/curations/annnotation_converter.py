"""
Class to facilitate Annotations to Labels conversion.

Author: Parker Hicks
Date: 2025-09-10

Last updated: 2025-09-10 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import numpy as np
import polars as pl

from metahq_core.curations.labels import Labels
from metahq_core.curations.propagator import Propagator
from metahq_core.ontology.graph import Graph
from metahq_core.util.helpers import flatten_list
from metahq_core.util.io import load_txt
from metahq_core.util.supported import onto_relations, ontologies

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations
    from metahq_core.util.alltypes import IdArray, NpIdArray


class AnnotationsConverter:
    def __init__(
        self,
        anno,
        _from,
        _to,
        reference,
        control_col="MONDO:0000000",
        group_col="group",
    ):
        self.anno: Annotations = anno
        self._from: IdArray = _from
        self._to: IdArray = _to
        self.reference: str = reference
        self.control_col: str = control_col
        self.group_col: str = group_col

    def to_labels(self, mode: Literal["propagate", "label"], terms: IdArray):
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
        graph = Graph.from_obo(ontologies(self.reference), ontology=self.reference)

        # setup annotations given the selected mode
        self._setup_mode(mode, terms, graph)

        # extract controls if they exist
        ctrl_ids = self._prepare_control_data(self.control_col)

        # get terms to propagate to
        terms_in_graph = self._prepare_terms(graph)
        propagate_to = self._prepare_targets(graph)

        # propagate
        labels = self._propagate_annotations()

        # handle controls
        if self.controls and ctrl_ids is not None and not self.anno.collapsed:
            print(f"Propagating {len(ctrl_ids)} controls.")
            ctrl_labels = self.propagate_controls(
                ctrl_ids, list(propagate_to), labels, self.group_col
            )
            labels = pl.concat([self.anno.ids, labels], how="horizontal")
            return Labels.from_df(
                labels.update(ctrl_labels, on=self.anno.index_col, how="full"),
                index_col=self.anno.index_col,
            )

        # combine IDs and labels
        return Labels(labels, self.anno.ids, self.anno.index_col, self.anno.group_cols)

    def _filter_to_system_descendants(self, graph: Graph, reference: str) -> NpIdArray:
        """Filter terms to system descendants if requested."""
        print(f"Propagating to {reference} system descendants.")
        systems = load_txt(onto_relations(reference, "systems"))
        _to = list(set(flatten_list(list(graph.descendants_from(systems).values()))))
        _, _, in_systems = np.intersect1d(_to, self.anno.entities, return_indices=True)
        return np.array(self.anno.entities)[in_systems]

    def _get_union_terms(self, graph: Graph):
        """Get intersection of graph nodes and current annotation columns."""
        _, _, in_graph = np.intersect1d(
            graph.nodes, self.anno.entities, return_indices=True
        )
        return np.array(self.anno.entities)[in_graph]

    def _prepare_control_data(self, ctrl_col: str):
        """Extract control data and update main data."""
        if ctrl_col not in self.anno.entities:
            return None

        self.controls = True
        # combine ids and data to filter controls
        combined = pl.concat([self.anno.ids, self.data], how="horizontal")
        ctrl_ids = combined.filter(pl.col(ctrl_col) == 1).select(self.anno.ids.columns)

        # Remove control column from main data
        self.data = self.data.drop(ctrl_col)

        return ctrl_ids

    def _prepare_targets(self, graph: Graph) -> list[str]:
        """Returns system descendants or all terms in the graph to propagate to."""
        if isinstance(self._to, str) and self._to == "system_descendants":
            return self._filter_to_system_descendants(graph, self.reference)

        if isinstance(self._to, str) and self._to == "all":
            return list(graph.nodes)

        supported: list[str] = ["system_descendants", "all"]
        raise ValueError(f"Expected to in {supported}, got {to}.")

    def _prepare_terms(self, graph: Graph) -> NpIdArray:
        """Prepare and validate terms for propagation."""
        return self._get_union_terms(graph)

    def _propagate_annotations(self):
        """Perform the actual annotation propagation."""
        print(
            f"Propagating {self.anno.n_indices} annotations to {len(_from)} terms to {len(_to)} terms."
        )
        # select terms to propagate to
        self.data = self.data.select(self._from)

        propagator = Propagator(
            self.reference, self.data.to_numpy(), self._from, self._to
        )
        return propagator.propagate()

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
                self.anno.index_col
            ].to_list()
            ctrl_anno = np.zeros((len(_controls), len(terms)), dtype=np.int32)

            _anno = labels_mat[np.where(np.array(self.anno.groups) == group)[0], :]
            positive_annos = np.any(_anno == 1, axis=0)

            ctrl_anno[:, positive_annos] = 2

            ctrl_dfs.append(
                ctrl_id.filter(pl.col("index").is_in(_controls))
                .select("index")
                .hstack(pl.DataFrame(ctrl_anno, schema=terms))
            )

        return pl.concat(ctrl_dfs).lazy()

    def _setup_mode(
        self, mode: Literal["propagate", "label"], _to: IdArray, graph: Graph
    ):
        """


        If mode is set to "propagate", this will turn the annotations
        curation into a index x _from table where _from are the users
        selected terms and all of their descendants.
        """
        if mode == "propagate":
            descendants = []
            _desc_map = graph.descendants_from(_to)
            for terms in _desc_map.values():
                descendants.extend(terms)

            _from = _to + descendants

            self.anno = self.anno.select(_from)

        if mode == "label":
            pass
