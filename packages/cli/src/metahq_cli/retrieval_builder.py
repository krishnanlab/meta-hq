"""
Class to take retrieval arguments, check them, and build the retrieval query.

Author: Parker Hicks
Date: 2025-10-16

Last updated: 2025-12-05 by Parker Hicks
"""

from typing import TYPE_CHECKING

import polars as pl
from metahq_core.util.exceptions import NoResultsFound
from metahq_core.util.supported import get_ontology_families

from metahq_cli.retriever import CurationConfig, OutputConfig, QueryConfig
from metahq_cli.util.checkers import (
    check_filter,
    check_filter_keys,
    check_format,
    check_if_txt,
    check_metadata,
    check_mode,
    check_outfile,
)
from metahq_cli.util.messages import TruncatedList
from metahq_cli.util.supported import required_filters

if TYPE_CHECKING:
    import logging


class Builder:
    """Class to build query, curation, and output configurations for `metahq retrieve`.
    Exists to support modularity and reduce redundnacy in the retrieval commands.
    """

    def __init__(self, logger, verbose=True):
        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def get_filters(self, filters: str) -> dict[str, str]:
        """Parses and checks requested filters.

        Arguments:
            filters (str):
                A comma-delimited string of supported MetaHQ filters.

        Returns:
            A dictionary of filter key, values.

        Examples:
            >>> from metahq_cli.retrieval_builder import Builder
            >>> builder = Builder()
            >>> filters = 'species=human,ecode=expert,tech=rnaseq'
            >>> builder.get_filters()
            {'species': 'human', 'ecode': 'expert', 'tech': 'rnaseq'}
        """
        _filters = self._parse_filters(filters)
        self.report_bad_filters(_filters)
        return _filters

    def parse_onto_terms(self, terms: list[str], reference: str) -> list[str]:
        """Collects passed query terms and checks if they are appropriate.

        Attributes:
            terms (list[str]):
                A list of ontology term IDs. Can be ontology IDs for tissues or diseases.
                (e.g., `['UBERON:0000948', 'UBERON:0000955']`).

            reference (str):
                An indication of what kind of terms these are (e.g., 'UBERON' or 'MONDO').

        Returns:
            Any terms that are supported within MetaHQ.

        Raises:
            `NoResultsFound` if none of the terms are in the MetaHQ database.
        """
        available = (
            pl.scan_parquet(get_ontology_families(reference)["relations"])
            .collect_schema()
            .names()
        )

        if terms == "all":
            return available

        parsed = self._parse(terms, available)

        # check if terms are missing
        diff = abs(len(terms) - len(parsed))
        if diff > 0:
            missing = list(set(terms) - set(parsed))
            if len(missing) > 10:
                missing = TruncatedList(missing)

            # fail if all are missing
            if diff == len(terms):
                msg = (
                    f"""Terms: {missing} have no annotations for: {reference.upper()}"""
                )

                if self.verbose:
                    self.log.error(msg)

                raise NoResultsFound(msg)

            # only raise warning if some are missing
            self.log.warning(
                "No annotations for input terms: %s",
                missing,
            )

        return parsed

    def query_config(
        self, db: str, attribute: str, level: str, filters: dict[str, str]
    ) -> QueryConfig:
        """Construct a query configuration.

        Query parameters are checked in the metahq_core.query module.

        Arguments:
            db (str):
                The name of a supported database within MetaHQ.

            attribute (str):
                A supported attribute within MetaHQ.

            level (str):
                A level of annotations (e.g., `'sample'` or `'series'`).

            filters (dict[str, str]):
                Filters parsed by `Builder.get_filters`.

        Returns:
            A populated `QueryConfig`.
        """
        check_filter("ecodes", filters["ecode"])
        check_filter("species", filters["species"])
        check_filter("technologies", filters["tech"])

        return QueryConfig(
            database=db,
            attribute=attribute,
            level=level,
            ecode=filters["ecode"],
            species=filters["species"],
            tech=filters["tech"],
        )

    def curation_config(self, terms: str, mode: str, ontology: str) -> CurationConfig:
        """Construct a curation configuration.

        Attributes:
            terms (str):
                A list of terms to curate annotations for.

            mode (str):
                A supported curation mode (e.g., `'annotate'`, `'label'`).

            ontology (str):
                An ontology to use for propagating annotations and assigning labels.

        Returns:
            A populated `CurationConfig`.
        """
        if ontology == "sex":
            return self.make_sex_curation(terms, mode)

        if ontology == "age":
            return self.make_age_curation(terms, mode)

        if terms == "all":
            _terms = self.parse_onto_terms(terms, ontology)
        else:
            _terms = check_if_txt(terms)

        if isinstance(_terms, str):
            _terms = _terms.split(",")

        _terms = self.parse_onto_terms(_terms, ontology)

        return CurationConfig(mode, _terms, ontology)

    def output_config(
        self, outfile: str, fmt: str, metadata: str, level: str
    ) -> OutputConfig:
        """Construct an output configuration.

        Attributes:
            outfile (str | Path):
                Path to file to store annotations.

            fmt (Literal["json", "parquet", "csv", "tsv"]):
                Format of the output file.

            metadata (str):
                Comma-delimited string indicating which metadata fields to include.

            level (str):
                Annotation level to check if any of the requested metadata fields are
                    available for the requested level.

        Returns:
            A populated `OutputConfig`.
        """
        check_metadata(level, metadata)
        check_format(fmt)
        check_outfile(outfile)

        return OutputConfig(outfile, fmt, metadata)

    def make_age_curation(self, terms: str, mode: str) -> CurationConfig:
        """Makes an age-specific CurationConfig."""
        _terms = check_if_txt(terms)
        check_mode("age", mode)

        if _terms == "all":
            from metahq_core.util.supported import supported

            _terms = supported("age_groups")

        elif isinstance(_terms, str):
            _terms = _terms.split(",")

        else:
            self.log.error("Invalid input: %s", terms)

        return CurationConfig(mode, _terms, ontology="age")

    def make_sex_curation(self, terms: str, mode: str):
        """Sex-specific curation."""
        _terms = check_if_txt(terms)
        check_mode("sex", mode)

        if _terms == "all":
            from metahq_core.util.supported import sexes

            _terms = sexes()

        elif isinstance(_terms, str):
            _terms = _terms.split(",")

        else:
            self.log.error("Invalid input: %s", terms)

        _terms = self._map_sex_to_id(_terms)
        return CurationConfig(mode, _terms, ontology="sex")

    def _map_sex_to_id(self, terms: list[str]):
        """Map male to M and female to F if passed."""
        opt = {"male": "M", "female": "F"}

        result = []
        for term in terms:
            if term in ["male", "female"]:
                result.append(opt[term])
            else:
                result.append(term)

        return result

    def _parse(self, terms: list[str], available: list[str]) -> list[str]:
        return [term for term in terms if term in available]

    def _parse_filters(self, filters: str) -> dict[str, str]:
        as_list: list[list[str]] = [f.split("=") for f in filters.split(",")]
        as_dict: dict[str, str] = {f[0]: f[1] for f in as_list}

        not_in_filters: list[str] = []
        for key in required_filters():
            if key not in as_dict:
                not_in_filters.append(key)

        if len(not_in_filters) > 0:
            msg: str = f"Missing required filters {not_in_filters}."
            self.log.error(msg)
            raise RuntimeError(msg)

        return as_dict

    def report_bad_filters(self, filters: dict[str, str]):
        """Check filters and return improper filter parameters."""
        bad_filters = check_filter_keys(filters)
        if len(bad_filters) > 0:
            msg = ("Expected filters in %s, got %s.", required_filters(), bad_filters)

            if self.verbose:
                self.log.error(msg)

            raise ValueError(msg)
