"""
Facilitates argument and curation parsing for metaHQ retrieval commands.

Author: Parker Hicks
Date: 2025-09-25

Last updated: 2025-09-26
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl
from metahq_core.query import Query
from metahq_core.util.progress import spinner
from metahq_core.util.supported import supported

from metahq_cli.util.messages import error, warning

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations
    from metahq_core.curations.labels import Labels


@dataclass
class QueryConfig:
    """Storage for query parameters."""

    database: str
    attribute: str
    level: str
    ecode: str
    species: str
    technology: str


@dataclass
class CurationConfig:
    """Storage for curation parameters."""

    mode: str
    terms: list[str]
    ontology: str


@dataclass
class OutputConfig:
    """Storage for output parameters."""

    outfile: str | Path
    fmt: Literal["json", "parquet", "csv", "tsv"]
    metadata: str


class Retriever:
    """
    Queries, curates, and saves MetaHQ annotations.
    Exists to reduce redundancy in MetaHQ retrieve commands.

    Attributes
    ----------
    query_config: QueryConfig
        Parameters for querying.

    curation_config: CurationConfig
        Parameters for curating annotations.

    output_config: OutputConfig
        Parameters for saving curations.

    Methods
    -------
    curate()
        Converts Annotations object to direct/propagated annotations
        or to labels.

    query()
        Performs a MetaHQ query given a config of filters.

    retrieve()
        Performs the pipeline of query -> curate -> save
    """

    def __init__(self, query_config, curation_config, output_config, verbose):
        self.query_config: QueryConfig = query_config
        self.curation_config: CurationConfig = curation_config
        self.output_config: OutputConfig = output_config

        self.verbose = verbose

    def curate(self, annotations: Annotations):
        """
        Mutate curations by specified mode.

        Currently not get a spinner wrapper. Rather, progress bars are shown
        for curation propagation steps.
        """
        return self._curate_by_mode(annotations)

    def query(self):
        """Performs a MetaHQ query"""
        if self.verbose:
            return self._query_verbose()

        return self._query_silent()

    def retrieve(self):
        """Performs the retrieval pipeline: query -> curate -> save."""
        curation = self.query()
        curation = self.curate(curation)
        self.save_curation(curation)

    def save_curation(self, curation: Annotations | Labels):
        """Saves the curation."""
        if self.verbose:
            self._save_verbose(curation)
        self._save_silent(curation)

    def _curate_by_mode(self, curation: Annotations) -> Annotations | Labels:
        """Apply the appropriate curation method to queried annotations."""
        if self.curation_config.mode == "direct":
            return self._direct_annotations(curation)

        if self.curation_config.mode == "propagate":
            return self._propagate_annotations(curation, mode=0)

        if self.curation_config.mode == "label":
            return self._propagate_annotations(curation, mode=1)

        error(
            f"Expected mode in {supported('modes')}, got {self.curation_config.mode}."
        )

    def _direct_annotations(self, curation: Annotations) -> Annotations:
        """Identify and return terms in the query that have annotations."""

        available_terms = self._filter_missing_entities(curation)

        return curation.select(available_terms).filter(
            pl.any_horizontal(pl.col(available_terms) == 1)
        )

    def _propagate_annotations(
        self, curation: Annotations, mode: Literal[0, 1]
    ) -> Annotations | Labels:
        """Wrapper for Annotations propagation."""

        result = curation.propagate(
            self.curation_config.terms,
            self.curation_config.ontology,
            mode=mode,
            verbose=self.verbose,
        )

        return result

    def _query(self) -> Annotations:
        return Query(
            database=self.query_config.database,
            attribute=self.query_config.attribute,
            level=self.query_config.level,
            ecode=self.query_config.ecode,
            species=self.query_config.species,
            technology=self.query_config.technology,
        ).annotations()

    def _query_silent(self):
        return self._query()

    def _filter_missing_entities(self, curation: Annotations | Labels) -> list[str]:
        terms_with_anno = [
            term for term in self.curation_config.terms if term in curation.entities
        ]
        not_in_anno = [
            term for term in self.curation_config.terms if not term in terms_with_anno
        ]

        if len(not_in_anno) == len(self.curation_config.terms):
            error(
                "No annotations for any terms. Try propagating or use different contitions."
            )

        if self.verbose:
            if len(terms_with_anno) != len(self.curation_config.terms):
                warning(
                    f"Warning: {not_in_anno} have no annotations. Try propagating or use different conditions."
                )
        return terms_with_anno

    @spinner(desc="Querying...", p_message="Querying...", end_message="Done")
    def _query_verbose(self):
        return self._query()

    def _save(self, curation):
        curation.save(
            outfile=self.output_config.outfile,
            fmt=self.output_config.fmt,
            metadata=self.output_config.metadata,
        )

    def _save_silent(self, curation):
        self._save(curation)

    @spinner(desc="Saving...", p_message="...", end_message="Done")
    def _save_verbose(self, curation):
        self._save(curation)
