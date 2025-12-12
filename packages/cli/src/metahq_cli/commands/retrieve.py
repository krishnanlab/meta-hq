"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-12-01 by Parker Hicks
"""

import click
from metahq_core.util.progress import get_console
from metahq_core.util.supported import get_log_dir, supported

from metahq_cli.logger import setup_logger
from metahq_cli.retrieval_builder import Builder
from metahq_cli.retriever import Retriever
from metahq_cli.util.common_args import (
    logging_args,
    ontology_retrieval_args,
    retrieval_args,
)
from metahq_cli.util.helpers import set_verbosity

AGE_GROUP_OPT = click.Choice(supported("age_groups") + ["all"])


def check_direct(mode: str, direct: bool, verbose: bool, logger) -> str:
    """Checks if direct flag was passed to return direct annotations.

    Parameters
    ----------
    mode: str
        The user's passed mode. Either annotate or label.

    direct: bool
        Value of the `direct` command argument.

    verbose: bool
        Verbosity yes or no.

    logger: logging.Logger
        Initialized Python Logger object.

    Returns
    -------
    'direct' if `direct` is True, otherwise mode.

    """
    if direct:
        if verbose:
            logger.info("Overriding passed mode '%s' with 'direct'.", mode)
        return "direct"
    return mode


# ===================================================
# ==== entry point
# ===================================================
@click.group
def retrieve_commands():
    """Retrieval commands for tissue, disease, sex, and age annotations."""


@retrieve_commands.command("age")
@click.option(
    "--terms",
    type=AGE_GROUP_OPT,
    default="all",
    help="Age groups to choose. Can combine like 'fetus,adult'.",
)
@retrieval_args
@logging_args
def retrieve_age(terms, level, fmt, metadata, filters, output, log_level, quiet):
    """Retrieval command for age group annotations."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=log_level, log_dir=get_log_dir()
    )

    builder = Builder(logger=log, verbose=verbose)

    # parse and check filters
    filters = builder.get_filters(filters)

    # make configs
    query_config = builder.query_config("geo", "age", level, filters)
    curation_config = builder.curation_config(terms, "direct", "age")
    output_config = builder.output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(
        query_config, curation_config, output_config, logger=log, verbose=verbose
    )
    retriever.retrieve()


@retrieve_commands.command("diseases")
@click.option("--terms", type=str, default="MONDO:0004994,MONDO:0018177")
@retrieval_args
@ontology_retrieval_args
@logging_args
def retrieve_diseases(
    terms, level, mode, fmt, metadata, filters, output, log_level, quiet, direct
):
    """Retrieval command for disease ontology terms."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=log_level, log_dir=get_log_dir()
    )

    # hidden from user. Used to test annotation quality.
    mode = check_direct(mode, direct, verbose, log)
    builder = Builder(logger=log, verbose=verbose)

    # parse and check filters
    filters = builder.get_filters(filters)

    # make configs
    query_config = builder.query_config("geo", "disease", level, filters)
    curation_config = builder.curation_config(terms, mode, "mondo")
    output_config = builder.output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(
        query_config, curation_config, output_config, logger=log, verbose=verbose
    )
    retriever.retrieve()


@retrieve_commands.command("sex")
@click.option("--terms", type=str, default="male,female")
@retrieval_args
@logging_args
def retrieve_sex(terms, level, fmt, metadata, filters, output, log_level, quiet):
    """Retrieval command for sex annotations."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=log_level, log_dir=get_log_dir()
    )

    builder = Builder(logger=log, verbose=verbose)

    # parse and check filters
    filters = builder.get_filters(filters)

    # make configs
    query_config = builder.query_config("geo", "sex", level, filters)
    curation_config = builder.curation_config(terms, "direct", "sex")
    output_config = builder.output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(
        query_config, curation_config, output_config, logger=log, verbose=verbose
    )
    retriever.retrieve()


@retrieve_commands.command("tissues")
@click.option("--terms", type=str, default="UBERON:0000948,UBERON:0000955")
@retrieval_args
@ontology_retrieval_args
@logging_args
def retrieve_tissues(
    terms, level, mode, fmt, metadata, filters, output, log_level, quiet, direct
):
    """Retrieval command for tissue ontology terms."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=log_level, log_dir=get_log_dir()
    )

    # hidden from user. Used to test annotation quality.
    mode = check_direct(mode, direct, verbose, log)
    builder = Builder(logger=log, verbose=verbose)

    # parse and check filters
    filters = builder.get_filters(filters)

    # make configs
    query_config = builder.query_config("geo", "tissue", level, filters)
    curation_config = builder.curation_config(terms, mode, "uberon")
    output_config = builder.output_config(output, fmt, metadata, level=level)

    # retrieve
    retriever = Retriever(
        query_config, curation_config, output_config, logger=log, verbose=verbose
    )
    retriever.retrieve()
