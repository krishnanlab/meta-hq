"""
Class to take retrieval arguments, check them, and build the retrieval query.

Author: Parker Hicks
Date: 2025-10-16

Last updated: 2025-11-19 by Parker Hicks
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
    def __init__(self, logger, verbose=True):
        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def get_filters(self, filters: str) -> dict[str, str]:
        _filters = self._parse_filters(filters)
        self.report_bad_filters(_filters)
        return _filters

    def _parse(self, terms: list[str], available: list[str]) -> list[str]:
        return [term for term in terms if term in available]

    def parse_onto_terms(self, terms: list[str], reference: str) -> list[str]:
        available = (
            pl.scan_parquet(get_ontology_families(reference)["relations"])
            .collect_schema()
            .names()
        )

        if terms == "all":
            return available

        parsed = self._parse(terms, available)

        if self.verbose:
            diff = abs(len(terms) - len(parsed))
            if diff > 0:
                missing = list(set(terms) - set(parsed))
                if len(missing) > 10:
                    missing = TruncatedList(missing)

                self.log.warning(
                    "No annotations for input terms: %s",
                    missing,
                )

        if len(parsed) == 0:
            msg = f"""{terms} have no annotations for ontology: {reference.upper()}.
Try propagating or use different conditions."""

            if self.verbose:
                self.log.error(msg)

            raise NoResultsFound(msg)

        return parsed

    def query_config(
        self, db: str, attribute: str, level: str, filters: dict[str, str]
    ):
        """
        Construct a query configuration.

        Query parameters are checked in the metahq_core.query module.
        """
        check_filter("ecodes", filters["ecode"])
        check_filter("species", filters["species"])
        check_filter("technologies", filters["technology"])

        return QueryConfig(
            database=db,
            attribute=attribute,
            level=level,
            ecode=filters["ecode"],
            species=filters["species"],
            technology=filters["technology"],
        )

    def make_sex_curation(self, terms: str, mode: str):
        _terms = check_if_txt(terms)
        check_mode("sex", mode)

        if _terms == "all":
            from metahq_core.util.supported import sexes

            _terms = sexes()

        elif isinstance(_terms, str):
            _terms = _terms.split(",")

        else:
            self.log.error("Invalid input: %s", terms)

        _terms = self.map_sex_to_id(_terms)
        return CurationConfig(mode, _terms, ontology="sex")

    def make_age_curation(self, terms: str, mode: str):
        _terms = check_if_txt(terms)
        check_mode("age", mode)

        if _terms == "all":
            from metahq_core.util.supported import age_groups

            _terms = age_groups()

        elif isinstance(_terms, str):
            _terms = _terms.split(",")

        else:
            self.log.error("Invalid input: %s", terms)

        return CurationConfig(mode, _terms, ontology="age")

    def curation_config(self, terms: str, mode: str, ontology: str):
        """Construct a curation configuration."""
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
        """Construct an output configuration."""
        check_metadata(level, metadata)
        check_format(fmt)
        check_outfile(outfile)

        return OutputConfig(outfile, fmt, metadata)

    def map_sex_to_id(self, terms: list[str]):
        """Map male to M and female to F if passed."""
        opt = {"male": "M", "female": "F"}

        result = []
        for term in terms:
            if term in ["male", "female"]:
                result.append(opt[term])
            else:
                result.append(term)

        return result

    def report_bad_filters(self, filters: dict[str, str]):
        """Check filters and return improper filter parameters."""
        bad_filters = check_filter_keys(filters)
        if len(bad_filters) > 0:
            msg = ("Expected filters in %s, got %s.", required_filters(), bad_filters)

            if self.verbose:
                self.log.error(msg)

            raise ValueError(msg)

    def set_verbosity(self, quiet: bool):
        if quiet:
            return False
        return True

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
