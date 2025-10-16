"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-10-16 by Parker Hicks
"""

import click
import polars as pl
from metahq_core.util.supported import age_groups, get_onto_families, ontologies

from metahq_cli.retriever import CurationConfig, OutputConfig, QueryConfig, Retriever
from metahq_cli.util.checkers import (
    check_filter,
    check_filter_keys,
    check_format,
    check_if_txt,
    check_metadata,
    check_mode,
    check_outfile,
)
from metahq_cli.util.helpers import FilterParser
from metahq_cli.util.messages import error
from metahq_cli.util.supported import required_filters

# ===================================================
# ==== helpers to build retrieval configurations
# ===================================================


def _parse(terms: list[str], available: list[str]) -> list[str]:
    return [term for term in terms if term in available]


def parse_onto_terms(terms: list[str], reference: str) -> list[str]:
    available = (
        pl.scan_parquet(get_onto_families(reference)["relations"])
        .collect_schema()
        .names()
    )

    if terms == "all":
        from metahq_core.ontology.base import Ontology

        onto = Ontology.from_obo(ontologies(reference), reference)
        return _parse(list(onto.class_dict.keys()), available)

    parsed = _parse(terms, available)

    if len(parsed) == 0:
        error(
            f"""{terms} have no annotations for ontology: {reference.upper()}.
Try propagating or use different conditions."""
        )

    return parsed


def make_query_config(db: str, attribute: str, level: str, filters: dict[str, str]):
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


def make_sex_curation(terms: str, mode: str):
    _terms = check_if_txt(terms)
    check_mode("sex", mode)

    if isinstance(_terms, str):
        _terms = _terms.split(",")

    _terms = map_sex_to_id(_terms)
    return CurationConfig(mode, _terms, ontology="sex")


def make_age_curation(terms: str, mode: str):
    _terms = check_if_txt(terms)
    check_mode("age", mode)

    if isinstance(_terms, str):
        _terms = _terms.split(",")

    return CurationConfig(mode, _terms, ontology="age")


def make_curation_config(terms: str, mode: str, ontology: str):
    """Construct a curation configuration."""
    if ontology == "sex":
        return make_sex_curation(terms, mode)

    if ontology == "age":
        return make_age_curation(terms, mode)

    if terms == "all":
        _terms = parse_onto_terms(terms, ontology)
    else:
        _terms = check_if_txt(terms)

    if isinstance(_terms, str):
        _terms = _terms.split(",")

    _terms = parse_onto_terms(_terms, ontology)

    return CurationConfig(mode, _terms, ontology)


def make_output_config(
    outfile: str, fmt: str, metadata: str, level: str
) -> OutputConfig:
    """Construct an output configuration."""
    check_metadata(level, metadata)
    check_format(fmt)
    check_outfile(outfile)

    return OutputConfig(outfile, fmt, metadata)


def map_sex_to_id(terms: list[str]):
    """Map male to M and female to F if passed."""
    opt = {"male": "M", "female": "F"}

    result = []
    for term in terms:
        if term in ["male", "female"]:
            result.append(opt[term])
        else:
            result.append(term)

    return result


def report_bad_filters(filters):
    """Check filters and return improper filter parameters."""
    bad_filters = check_filter_keys(filters)
    if len(bad_filters) > 0:
        exc = click.ClickException("Unsupported filter argument")
        exc.add_note(f"Expected filters in {required_filters()}, got {bad_filters}.")


def set_verbosity(quiet: bool):
    if quiet:
        return False
    return True


# ===================================================
# ==== entry point
# ===================================================
@click.group
def retrieve_commands():
    """Retrieval commands for tissue, disease, sex, and age annotations."""


@retrieve_commands.command("tissues")
@click.option("--terms", type=str, default="UBERON:0000948,UBERON:0000955")
@click.option("--level", type=click.Choice(["sample", "series"]))
@click.option("--mode", type=click.Choice(["direct", "propagate", "label"]))
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert-curated,technology=microarray",
)
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--fmt", type=str, default="parquet")
@click.option("--metadata", type=str, default="df")
@click.option("--quiet", is_flag=True, default=False)
def retrieve_tissues(terms, level, mode, fmt, metadata, filters, output, quiet):
    """Retrieval command for tissue ontology terms."""
    if metadata == "df":
        metadata = level

    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "tissue", level, filters)
    curation_config = make_curation_config(terms, mode, "uberon")
    output_config = make_output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(query_config, curation_config, output_config, verbose)
    retriever.retrieve()


@retrieve_commands.command("diseases")
@click.option("--terms", type=str, default="MONDO:0004994,MONDO:0018177")
@click.option("--level", type=click.Choice(["sample", "series"]))
@click.option("--mode", type=click.Choice(["direct", "propagate", "label"]))
@click.option("--fmt", type=str, default="parquet")
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert-curated,technology=rnaseq",
)
@click.option("--metadata", type=str, default="df")
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--quiet", is_flag=True, default=False)
def retrieve_diseases(terms, level, mode, fmt, metadata, filters, output, quiet):
    """Retrieval command for disease ontology terms."""
    if metadata == "df":
        metadata = level

    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "disease", level, filters)
    curation_config = make_curation_config(terms, mode, "mondo")
    output_config = make_output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(query_config, curation_config, output_config, verbose)
    retriever.retrieve()


@retrieve_commands.command("sex")
@click.option("--terms", type=str, default="male,female")
@click.option("--level", type=click.Choice(["sample", "series"]))
@click.option("--mode", type=click.Choice(["direct", "propagate", "label"]))
@click.option("--fmt", type=str, default="parquet")
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert-curated,technology=rnaseq",
)
@click.option("--metadata", type=str, default="df")
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--quiet", is_flag=True, default=False)
def retrieve_sex(terms, level, mode, fmt, metadata, filters, output, quiet):
    """Retrieval command for sex annotations."""
    if metadata == "df":
        metadata = level

    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "sex", level, filters)
    curation_config = make_curation_config(terms, mode, "sex")
    output_config = make_output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(query_config, curation_config, output_config, verbose)
    retriever.retrieve()


@retrieve_commands.command("age")
@click.option(
    "--terms",
    type=str,
    help=f"Choose from {age_groups()}. Can combine like 'fetus,adult'.",
)
@click.option(
    "--level", type=click.Choice(["sample", "series"]), help="GEO annotation level."
)
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert,technology=rnaseq",
    help="Filters for species, ecode, and technology. Run `metahq supported` for options.",
)
@click.option("--metadata", type=str, default="default")
@click.option("--fmt", type=str, default="parquet")
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--quiet", is_flag=True, default=False)
def retrieve_age(terms, level, fmt, metadata, filters, output, quiet):
    """Retrieval command for age group annotations."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "age", level, filters)
    curation_config = make_curation_config(terms, "direct", "age")
    output_config = make_output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(query_config, curation_config, output_config, verbose)
    retriever.retrieve()
