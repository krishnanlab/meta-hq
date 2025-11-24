"""
Definition of common arguments across CLI commands. Exists to
remove redundancy and improve readability in CLI command
definitions.

Author: Parker Hicks
Date: 2025-11-24

Last updated: 2025-11-24 by Parker Hicks
"""

from functools import wraps

import click
from metahq_core.util.supported import supported

FMT_OPT = click.Choice(supported("formats"))
LEVEL_OPT = click.Choice(supported("levels"))
LOGLEVEL_OPT = click.Choice(supported("log_levels"))
MODE_OPT = click.Choice(supported("modes"))


def logging_args(command):
    """Decorator to assign logging arguments across CLI commands."""

    @click.option(
        "--log-level", type=LOGLEVEL_OPT, default="info", help="Logging level."
    )
    @click.option(
        "--quiet",
        is_flag=True,
        default=False,
        help="No log or console output if applied.",
    )
    @wraps(command)
    def wrapper(*args, **kwargs):
        return command(*args, **kwargs)

    return wrapper


def retrieval_args(command):
    """Decorator to assign common arguments across retrieval commands."""

    @click.option(
        "--level", type=LEVEL_OPT, default="sample", help="GEO annotation level."
    )
    @click.option(
        "--filters",
        type=str,
        default="species=human,ecode=expert,tech=rnaseq",
        help="Filters for species, ecode, and technology. Run `metahq supported` for options.",
    )
    @click.option(
        "--output",
        type=click.Path(),
        default="annotations.parquet",
        help="Path to outfile.",
    )
    @click.option("--fmt", type=FMT_OPT, default="parquet")
    @click.option("--metadata", type=str, default="default")
    @wraps(command)
    def wrapper(*args, **kwargs):
        return command(*args, **kwargs)

    return wrapper


def ontology_retrieval_args(command):
    """
    Decorator to assign common arguments specific to retrival for
    ontology-based queries like tisues and diseases
    """

    @click.option(
        "--mode",
        type=MODE_OPT,
        default="annotate",
        help="Retrieve annotations or labels.",
    )
    @click.option(
        "--direct",
        is_flag=True,
        default=False,
        help="Get direct annotations. Hidden because not practical.",
        hidden=True,
    )
    @wraps(command)
    def wrapper(*args, **kwargs):
        return command(*args, **kwargs)

    return wrapper
