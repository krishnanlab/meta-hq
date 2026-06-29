"""
Facilitates argument and curation parsing for metaHQ retrieval commands.

Author: Parker Hicks
Date: 2025-09-25

Last updated: 2026-04-01 by Parker Hicks
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl
from metahq_core.export.references import CitationConfig
from metahq_core.export.refinebio import DATA_CART_URL, RefineBioExporter
from metahq_core.query import Query
from metahq_core.util.exceptions import NoResultsFound
from metahq_core.util.supported import database_ids, metadata_fields, supported

from metahq_cli.util.messages import TruncatedList

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.annotations import Annotations
    from metahq_core.curations.labels import Labels


@dataclass
class QueryConfig:
    """Storage for query parameters.

    Attributes:
        database (str):
            The name of a supported database within MetaHQ.

        attribute (str):
            A supported attribute within MetaHQ.

        level (str):
            A level of annotations (e.g., 'sample' or 'series').

        ecode (str):
            Evidence code (e.g., 'expert', 'crowd', 'any')

        species (str):
            A supported species within MetaHQ.

        tech (str):
            A supported technology within MetaHQ.
    """

    database: str
    attribute: str
    level: str
    ecode: str
    species: str
    tech: str
    license: str = "any"


@dataclass
class CurationConfig:
    """Storage for curation parameters.

    Attributes:
        mode (str):
            A supported curation mode (e.g., 'annotate', 'label').

        terms (str):
            A list of terms to curate annotations for.

        ontology (str):
            An ontology to use for propagating annotations and assigning labels.
    """

    mode: str
    terms: list[str]
    ontology: str


@dataclass
class OutputConfig:
    """Storage for output parameters.

    Attributes:
        outfile (str | Path):
            Path to file to store annotations.

        fmt (Literal["json", "parquet", "csv", "tsv"]):
            Format of the output file.

        metadata (str):
            Comma-delimited string indicating which metadata fields to include.

        attribute (str):
            A supported attribute within MetaHQ.

        level (str):
            A level of annotations (e.g., 'sample' or 'series').
    """

    outfile: str | Path
    fmt: Literal["json", "parquet", "csv", "tsv"]
    metadata: str
    attribute: str
    level: str


class Retriever:
    """
    Queries, curates, and saves MetaHQ annotations for `metahq retrieve`.
    Exists to reduce redundancy in MetaHQ retrieve commands.

    Attributes:
        query_config: QueryConfig
            Parameters for querying.

        curation_config: CurationConfig
            Parameters for curating annotations.

        output_config: OutputConfig
            Parameters for saving curations.

        citation_config: CitationConfig
            Parameters for saving citations.

        refinebio: bool
            If True, creates a refine.bio dataset from the retrieved curation and
            submits it through refine.bio's dataset API.
    """

    def __init__(
        self,
        query_config,
        curation_config,
        output_config,
        citation_config,
        logger,
        verbose=True,
        refinebio=False,
    ):
        self.query_config: QueryConfig = query_config
        self.curation_config: CurationConfig = curation_config
        self.output_config: OutputConfig = output_config
        self.citation_config: CitationConfig = citation_config
        self.refinebio: bool = refinebio

        self.log: logging.Logger = logger
        self.verbose: bool = verbose

        if verbose:
            self.log.debug(
                "Using configs:\n%s\n%s\n%s",
                self.query_config,
                self.curation_config,
                self.output_config,
            )

    def curate(self, annotations: Annotations) -> Annotations | Labels:
        """Mutate curations by specified mode.

        Arguments:
            annotations: Annotations
                A populated Annotations object.

        Returns:
            A populated Annotations or Labels object given the specified curation mode.

        Raises:
            Error: If there are no annotations for a set of query parameters.
        """
        self._check_terms_available(annotations)
        self._check_filters_results(annotations)

        if self.verbose:
            self.log.info("Curating...")

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

        if self.refinebio:
            self.include_refinebio_metadata()
            self.create_refinebio_dataset(curation)

        self.save_curation(curation)

    def include_refinebio_metadata(self):
        """Ensures refine.bio ID fields supported at the output level are
        requested in the output metadata, so the save step merges refine.bio
        sample/experiment IDs into the saved curation.
        """
        fields = [
            field
            for field in database_ids("refinebio")
            if field in metadata_fields(self.output_config.level)
        ]
        current = [f for f in self.output_config.metadata.split(",") if f]
        missing = [field for field in fields if field not in current]

        if missing:
            self.output_config.metadata = ",".join(current + missing)

    def create_refinebio_dataset(self, curation: Annotations | Labels):
        """Creates a refine.bio dataset from the retrieved curation.

        Arguments:
            curation (Annotations | Labels):
                A populated Annotations or Labels object to submit to refine.bio.
        """
        if self.verbose:
            self.log.info("Creating refine.bio dataset...")

        result = RefineBioExporter(
            logger=self.log, verbose=self.verbose
        ).create_dataset(curation)
        self.citation_config.refinebio_dataset_id = DATA_CART_URL + result["id"]

    def save_curation(self, curation: Annotations | Labels):
        """Saves the curation.

        Arguments:
            curation (Annotations | Labels):
                A populated Annotations or Labels object to save.
        """
        self._save(curation)

    def _curate_by_mode(self, curation: Annotations) -> Annotations | Labels:
        """Apply the appropriate curation method to queried annotations."""
        if self.curation_config.mode == "direct":
            return self._direct_annotations(curation)

        if self.curation_config.mode == "annotate":
            return self._propagate_annotations(curation, mode=0)

        if self.curation_config.mode == "label":
            return self._propagate_annotations(curation, mode=1)

        msg = (
            "Expected mode in %s, got %s.",
            supported("modes"),
            self.curation_config.mode,
        )

        if self.verbose:
            self.log.error(msg)

        raise ValueError(msg)

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
        )

        return result

    def _query(self) -> Annotations:
        if self.verbose:
            self.log.info("Querying...")

        return Query(
            database=self.query_config.database,
            attribute=self.query_config.attribute,
            level=self.query_config.level,
            ecode=self.query_config.ecode,
            species=self.query_config.species,
            technology=self.query_config.tech,
            license=self.query_config.license,
            logger=self.log,
            verbose=self.verbose,
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
            msg = "No annotations for any terms. Try using different conditions."
            self.log.error(msg)
            raise NoResultsFound(msg)

        if self.verbose:
            if len(terms_with_anno) != len(self.curation_config.terms):
                if len(not_in_anno) > 10:
                    not_in_anno = TruncatedList(not_in_anno)
                self.log.warning(
                    "Queries: %s have no annotations. Try using different conditions.",
                    not_in_anno,
                )
        return terms_with_anno

    def _query_verbose(self):
        return self._query()

    def _save(self, curation):

        if self.verbose:
            self.log.info(
                "Saving results to %s",
                Path(self.output_config.outfile).parent,
            )

        curation.save(
            outfile=self.output_config.outfile,
            fmt=self.output_config.fmt,
            metadata=self.output_config.metadata,
            attribute=self.output_config.attribute,
            level=self.output_config.level,
            citation_config=self.citation_config,
        )

    def _check_terms_available(self, annotations: Annotations) -> None:
        not_available = []
        for term in self.curation_config.terms:
            if term not in annotations.entities:
                not_available.append(term)

        if len(not_available) == len(self.curation_config.terms):
            self.log.error("No annotations available for your queried terms.")
            sys.exit(1)

    def _check_filters_results(self, annotations: Annotations) -> None:
        """If terms are in MetaHQ, but there are no samples returned"""
        if annotations.n_indices == 0:
            msg = (
                "No annotations for any terms given your filter parameters."
                " Try using different filter values."
            )
            self.log.error(msg)
            sys.exit(1)
