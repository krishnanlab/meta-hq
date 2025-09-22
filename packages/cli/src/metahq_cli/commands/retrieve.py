"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-05 by Parker Hicks
"""

import click
import polars as pl
from metahq_core.ontology.base import Ontology
from metahq_core.query import Query
from metahq_core.util.supported import ontologies

from metahq_cli.util.checkers import check_filters
from metahq_cli.util.helpers import FilterParser
from metahq_cli.util.supported import FILTERS


def parse_terms(terms: str, reference: str):
    onto = Ontology.from_obo(ontologies(reference), reference)
    if terms == "all":
        return list(onto.class_dict.keys())
    return terms.split(",")


@click.group
def retrieve_commands():
    """Retrieval commands for tissue, disease, sex, and age annotations."""


@retrieve_commands.command("tissues")
@click.option("--terms", type=str, default="UBERON:0000948,UBERON:0000955")
@click.option("--level", type=click.Choice(["sample", "series"]))
@click.option("--mode", type=click.Choice(["direct", "propagate", "label"]))
@click.option(
    "--filters", type=str, default="species=human,db=geo,ecode=expert-curated"
)
@click.option("--output", type=click.Path(), default="annotations.parquet")
@click.option("--fmt", type=str, default="parquet")
@click.option("--metadata", type=str)
def retrieve_tissues(terms, level, mode, fmt, metadata, filters, output):
    """Retrieval command for tissue ontology terms."""
    ontology = "uberon"
    terms = parse_terms(terms, ontology)
    filters = FilterParser.from_str(filters).filters

    bad_filters = check_filters(filters)
    if len(bad_filters) > 0:
        exc = click.ClickException("Unsupported filter argument")
        exc.add_note(f"Expected filters in {FILTERS}, got {bad_filters}.")

    curation = Query(
        filters["db"], "tissue", filters["ecode"], filters["species"]
    ).annotations()

    if mode == "direct":
        curation = curation.select(terms).filter(pl.any_horizontal(pl.col(terms) == 1))
    elif mode == "propagate":
        curation = curation.propagate(terms, ontology, mode=0)

    elif mode == "label":
        curation = curation.propagate(terms, ontology, mode=1)

    else:
        exc = click.ClickException("Unsupported mode argument")
        exc.add_note(f"Expected mode in [direct, propagated, labels], got {mode}")

    curation.save(output, fmt, metadata)


@retrieve_commands.command("diseases")
@click.option("--terms", type=str, default="MONDO:0004994,MONDO:0018177")
@click.option("--level", type=click.Choice(["sample", "series"]))
@click.option("--mode", type=click.Choice(["direct", "propagate", "label"]))
@click.option("--fmt", type=str, default="parquet")
@click.option(
    "--filters", type=str, default="species=human,db=geo,ecode=expert-curated"
)
@click.option("--metadata", type=str)
@click.option("--output", type=click.Path(), default="annotations.parquet")
def retrieve_diseases(terms, level, mode, fmt, metadata, filters, output):
    """Retrieval command for disease ontology terms."""
    ontology = "mondo"
    terms = parse_terms(terms, ontology)
    filters = FilterParser.from_str(filters).filters

    bad_filters = check_filters(filters)
    if len(bad_filters) > 0:
        exc = click.ClickException("Unsupported filter argument")
        exc.add_note(f"Expected filters in {FILTERS}, got {bad_filters}.")

    curation = Query(
        database=filters["db"],
        attribute="disease",
        level=level,
        ecode=filters["ecode"],
        species=filters["species"],
    ).annotations()

    if mode == "direct":
        curation = curation.select(terms).filter(pl.any_horizontal(pl.col(terms) == 1))
    elif mode == "propagate":
        curation = curation.propagate(terms, ontology, mode=0)

    elif mode == "label":
        curation = curation.propagate(terms, ontology, mode=1)

    else:
        exc = click.ClickException("Unsupported mode argument")
        exc.add_note(f"Expected mode in [direct, propagated, labels], got {mode}")

    curation.save(output, fmt, metadata)


@retrieve_commands.command("sex")
@click.option("--terms", type=str, default="male,female")
@click.option(
    "--filters", type=str, default="species=human,db=geo,ecode=expert-curated"
)
def retrieve_sex(terms, fmt, include_metadata, filters, output):
    pass


@retrieve_commands.command("age")
@click.option("--terms", type=str, default="10-20,70-80")
@click.option(
    "--filters", type=str, default="species=human,db=geo,ecode=expert-curated"
)
def retrieve_age(terms, fmt, include_metadata, filters, output):
    pass
