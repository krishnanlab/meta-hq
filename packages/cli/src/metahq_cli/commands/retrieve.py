"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-05 by Parker Hicks
"""

import click
from metahq_core.query import Query

from metahq_cli.util.helpers import FilterParser


@click.group
def retrieve_commands():
    """Retrieval commands for tissue, disease, sex, and age annotations."""
    pass


@retrieve_commands.command("tissues")
@click.option("--terms", type=str, default="UBERON:0000948,CL:0000187")
@click.option("--propagate", is_flag=True, defualt=False)
@click.option(
    "--filters", type=str, default="species=human,db=geo,ecode=expert-curated"
)
def retrieve_tissues(terms, propagate, fmt, include_metadata, filters, output):
    pass


@retrieve_commands.command("diseases")
@click.option("--terms", type=str, default="UBERON:0000948,CL:0000187")
@click.option("--propagate", is_flag=True, defualt=False)
@click.option(
    "--filters", type=str, default="species=human,db=geo,ecode=expert-curated"
)
def retrieve_diseases(terms, propagate, fmt, include_metadata, filters, output):
    pass


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
