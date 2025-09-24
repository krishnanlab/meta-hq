"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-23 by Parker Hicks
"""

import sys

import click
import polars as pl
from metahq_core.ontology.base import Ontology
from metahq_core.query import Query
from metahq_core.util.supported import ontologies

from metahq_cli.util.checkers import check_filters
from metahq_cli.util.helpers import FilterParser
from metahq_cli.util.supported import REQUIRED_FILTERS


def parse_terms(terms: str, reference: str):
    onto = Ontology.from_obo(ontologies(reference), reference)
    if terms == "all":
        return list(onto.class_dict.keys())
    return terms.split(",")


def warning(message):
    click.secho(f"WARNING: {message}", fg="yellow")


def error(message):
    click.secho(f"ERROR: {message}", fg="red")


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
@click.option("--metadata", type=str)
def retrieve_tissues(terms, level, mode, fmt, metadata, filters, output):
    """Retrieval command for tissue ontology terms."""
    ontology = "uberon"
    terms = parse_terms(terms, ontology)
    filters = FilterParser.from_str(filters).filters

    bad_filters = check_filters(filters)
    if len(bad_filters) > 0:
        exc = click.ClickException("Unsupported filter argument")
        exc.add_note(f"Expected filters in {REQUIRED_FILTERS}, got {bad_filters}.")

    curation = Query(
        database="geo",
        attribute="tissue",
        level=level,
        ecode=filters["ecode"],
        species=filters["species"],
        technology=filters["technology"],
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
    "--filters",
    type=str,
    default="species=human,ecode=expert-curated,technology=rnaseq",
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
        exc.add_note(f"Expected filters in {REQUIRED_FILTERS}, got {bad_filters}.")

    curation = Query(
        database="geo",
        attribute="disease",
        level=level,
        ecode=filters["ecode"],
        species=filters["species"],
        technology=filters["technology"],
    ).annotations()

    if mode == "direct":
        terms_with_anno = [term for term in terms if term in curation.entities]
        not_in_anno = [term for term in terms if not term in terms_with_anno]
        if len(not_in_anno) == len(terms):
            error(
                "No direct annotations for any terms. Try propagating or different contitions."
            )
            click.echo("Exiting...")
            sys.exit(1)

        if len(terms_with_anno) != len(terms):
            warning(
                f"Warning: {not_in_anno} have no direct annotations. Try propagating or different conditions."
            )

        curation = curation.select(terms_with_anno).filter(
            pl.any_horizontal(pl.col(terms_with_anno) == 1)
        )
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
