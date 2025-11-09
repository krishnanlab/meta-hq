"""
Class to facilitate Annotations propagation and convertion to labels.

Author: Parker Hicks
Date: 2025-09-10

Last updated: 2025-10-16 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

# from metahq_core.curations._multiprocess_propagator
from metahq_core.curations.labels import Labels
from metahq_core.curations.propagator import Propagator, propagate_controls
from metahq_core.logger import setup_logger
from metahq_core.ontology.graph import Graph
from metahq_core.util.helpers import merge_list_values
from metahq_core.util.progress import progress_wrapper
from metahq_core.util.supported import ontologies

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.annotations import Annotations


WARNING_SIZE = 1000


class AnnotationsConverter:
    """
    Helper class to convert annotations to propagated annotations
    or to labels.

    Attributes
    ----------
    anno: Annotations
        A MetaHQ Annotations object with columns of ontology terms
        rows as samples, and each value is a 1 or 0 indicating if a sample is
        annotated to a particular term.

    ontology: str
        The name of an ontology supported by MetaHQ.

    control_col: str
        Column name for control annotations.

    graph: Graph
        MetaHQ ontology Graph object.

    Methods
    -------
    propagate_up()
        Propagates annotations up to all terms in the annotations curation.
        If an index is annotated to a descendant of a term in `to`, then it
        is given an annotation of 1 to that term.

    to_labels()
        Converts annotations to -1, 0, +1 labels (and 2 if controls are present).

    """

    def __init__(
        self,
        anno,
        to_terms,
        ontology,
        control_col="MONDO:0000000",
        logger=setup_logger(__name__),
        verbose=False,
    ):
        self.anno: Annotations = anno
        self.to_terms: list[str] = to_terms
        self.ontology: str = ontology

        self.controls = False
        self.control_col: str = control_col

        self.graph = Graph.from_obo(ontologies(ontology), ontology=ontology)

        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def propagate_up(self):
        """
        Propagate annotations up to selected terms.

        """
        self._setup_to_annotate()

        if self.verbose:
            self.log.info("Propagating annotations up...")
            self.log.debug(
                "Propagating to %s terms in %s", len(self.to_terms), self.ontology
            )
        propagator = Propagator(
            self.ontology,
            self.anno,
            self.to_terms,
            relatives=["ancestors"],
            logger=self.log,
            verbose=self.verbose,
        )
        new, cols, ids = propagator.propagate_up()

        return pl.DataFrame(new, schema=cols), ids

    def to_labels(self, groups: str = "group"):
        """
        Convert annotations to propagated labels.

        If controls are present, they will be assigned a label of 2 for any
        terms that are labeled as positive for any of the samples that come
        from the same study as the control samples.

        Parameters
        ----------
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
        self._setup_to_label()

        # extract controls if they exist
        ctrl_ids = self._prepare_control_data()

        label_mat, cols, ids = self._make_labels()
        labels_df = ids.hstack(pl.DataFrame(label_mat, schema=cols))

        # handle controls
        if self.controls and ctrl_ids is not None and not self.anno.collapsed:

            self.log.info("Propagating controls...")

            labels_df = propagate_controls(
                labels_df, self.to_terms, "sample", "series", ctrl_ids
            )

            return Labels.from_df(
                labels_df,
                self.anno.index_col,
                tuple(self.anno.group_cols),
                logger=self.log,
                verbose=self.verbose,
            )

        return Labels.from_df(
            labels_df,
            self.anno.index_col,
            tuple(self.anno.group_cols),
            logger=self.log,
            verbose=self.verbose,
        )

    def _get_graph_relations(self, relatives: str, total=None) -> list[str]:
        """Get all ancestors or descendants of a list of ontology terms."""
        opt = {
            "ancestors": self.graph.ancestors_from,
            "descendants": self.graph.descendants_from,
        }
        if total is None:
            total = len(self.to_terms)

        if total > WARNING_SIZE:
            relation_map = progress_wrapper(
                f"{relatives}...",
                verbose=self.verbose,
                total=total,
                func=opt[relatives],
                nodes=self.to_terms,
                padding="    ",
            )
        else:
            relation_map = opt[relatives](nodes=self.to_terms)

        return merge_list_values(relation_map)

    def _prepare_control_data(self):
        """Extract control data and update main data."""
        if self.control_col not in self.anno.entities:
            return None

        self.controls = True

        # combine ids and data to filter controls
        ctrl_ids = self.anno.filter(pl.col(self.control_col) == 1).ids
        self.anno = self.anno.drop(self.control_col)

        return ctrl_ids

    def _setup_to_annotate(self):
        """
        This will turn the annotations curation into a index x _from table
        where _from are the users selected terms and all of their descendants.
        This will also remove samples annotated to terms that have no relation
        to the selected terms and their descendants.

        Annotations to the descendants of the specified terms are included to
        propagate them up to those terms.

        """
        descendants = self._get_graph_relations("descendants")

        _from = list(set(self.to_terms + descendants))
        _from = [term for term in _from if term in self.anno.entities]

        self.anno = self.anno.select(_from).filter(
            pl.any_horizontal(pl.col(_from) == 1)
        )

    def _setup_to_label(self):
        """
        Ancestors and descendants of the users selected terms are selected.
        No samples are removed this is to identify if samples are ancestors
        or descendants of the selected terms. If not, then they are assigned
        a -1 label. If samples are annotated to ancestors of the selected terms,
        they are assigned a 0 label.
        """
        total = len(self.to_terms)

        if self.verbose:
            self.log.info("Extracting ontology relationships for %s terms...", total)

            if total > WARNING_SIZE:
                self.log.info("This may take up to a minute.")

        descendants = self._get_graph_relations("descendants")
        ancestors = self._get_graph_relations("ancestors")

        _from = list(set(self.to_terms + descendants + ancestors))
        _from = [term for term in _from if term in self.anno.entities]

        if self.control_col in self.anno.entities:
            _from.append(self.control_col)

        self.anno = self.anno.select(_from)

    def _make_labels(self):
        """
        Propagates up to to_terms and down to to_terms. If any samples are not
        annotated to any descendants OR ancestors of to_terms, then they are
        assigned a -1.
        """
        if self.verbose:
            self.log.info("Converting annotations to labels...")
            self.log.debug(
                "Labeling %s entries to %s terms in %s",
                self.anno.n_indices,
                len(self.to_terms),
                self.ontology.upper(),
            )

        propagator = Propagator(
            self.ontology,
            self.anno,
            self.to_terms,
            relatives=["ancestors", "descendants"],
            verbose=self.verbose,
        )
        up_mat, cols, up_ids = propagator.propagate_up(verbose=self.verbose)
        down_mat, _, _ = propagator.propagate_down(verbose=self.verbose)

        neg_mask = (up_mat == 0) & (down_mat == 0)
        up_mat[neg_mask] = -1

        return up_mat, cols, up_ids
