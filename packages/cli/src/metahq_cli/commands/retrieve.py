"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-26 by Parker Hicks
"""

import click
import polars as pl
from metahq_core.util.supported import get_onto_families, ontologies

from metahq_cli.retriever import CurationConfig, OutputConfig, QueryConfig, Retriever
from metahq_cli.util.checkers import check_filters
from metahq_cli.util.helpers import FilterParser
from metahq_cli.util.messages import warning
from metahq_cli.util.supported import REQUIRED_FILTERS


# ===================================================
# ==== helpers to build retrieval configurations
# ===================================================
def parse_onto_terms(terms: str, reference: str):
    available = (
        pl.scan_parquet(get_onto_families(reference)["relations"])
        .collect_schema()
        .names()
    )

    parse = lambda terms: [term for term in terms if term in available]

    if terms == "all":
        from metahq_core.ontology.base import Ontology

        onto = Ontology.from_obo(ontologies(reference), reference)
        return parse(list(onto.class_dict.keys()))

    return parse(terms.split(","))


def make_query_config(db: str, attribute: str, level: str, filters: dict[str, str]):
    """Construct a query configuration."""
    return QueryConfig(
        database=db,
        attribute=attribute,
        level=level,
        ecode=filters["ecode"],
        species=filters["species"],
        technology=filters["technology"],
    )


def make_curation_config(terms: str, mode: str, ontology: str):
    """Construct a curation configuration."""
    if ontology == "sex":
        if mode != "direct":
            warning("Sex queries must be direct annotations.")
            print("Changing mode to direct...")

        _terms = map_sex_to_id(terms.split(","))

    else:
        _terms = parse_onto_terms(terms, ontology)

    return CurationConfig(mode, _terms, ontology)


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
    bad_filters = check_filters(filters)
    if len(bad_filters) > 0:
        exc = click.ClickException("Unsupported filter argument")
        exc.add_note(f"Expected filters in {REQUIRED_FILTERS}, got {bad_filters}.")


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
@click.option("--metadata", type=str, default="sample")
@click.option("--quiet", is_flag=True)
def retrieve_tissues(terms, level, mode, fmt, metadata, filters, output, quiet):
    """Retrieval command for tissue ontology terms."""
    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "tissue", level, filters)
    curation_config = make_curation_config(terms, mode, "uberon")
    output_config = OutputConfig(output, fmt, metadata)

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
@click.option("--metadata", type=str, default="sample")
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--quiet", is_flag=True, default=False)
def retrieve_diseases(terms, level, mode, fmt, metadata, filters, output, quiet):
    """Retrieval command for disease ontology terms."""
    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "disease", level, filters)
    curation_config = make_curation_config(terms, mode, "mondo")
    output_config = OutputConfig(output, fmt, metadata)

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
@click.option("--metadata", type=str, default="sample")
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--quiet", is_flag=True, default=False)
def retrieve_sex(terms, level, mode, fmt, metadata, filters, output, quiet):
    """Retrieval command for sex annotations."""
    verbose = set_verbosity(quiet)

    # parse and check filters
    filters = FilterParser.from_str(filters).filters
    report_bad_filters(filters)

    # make configs
    query_config = make_query_config("geo", "sex", level, filters)
    curation_config = make_curation_config(terms, mode, "sex")
    output_config = OutputConfig(output, fmt, metadata)

    # retrieve
    retriever = Retriever(query_config, curation_config, output_config, verbose)
    retriever.retrieve()


@retrieve_commands.command("age")
@click.option("--terms", type=str, default="10-20,70-80")
@click.option("--filters", type=str, default="species=human,ecode=expert-curated")
def retrieve_age(terms, fmt, include_metadata, filters, output):
    pass
