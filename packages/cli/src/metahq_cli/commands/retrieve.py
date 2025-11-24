"""
CLI command to retrieve annotations and labels from meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-11-24 by Parker Hicks
"""

import click
from metahq_core.util.progress import get_console
from metahq_core.util.supported import get_log_dir, supported

from metahq_cli.logger import setup_logger
from metahq_cli.retrieval_builder import Builder
from metahq_cli.retriever import Retriever

LOGLEVEL_OPT = click.Choice(supported("log_levels"))
FMT_OPT = click.Choice(supported("formats"))
LEVEL_OPT = click.Choice(supported("levels"))
MODE_OPT = click.Choice(supported("modes"))
AGE_GROUP_OPT = click.Choice(supported("age_groups") + ["all"])


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
@click.option("--level", type=LEVEL_OPT, default="sample", help="GEO annotation level.")
@click.option(
    "--mode", type=MODE_OPT, default="annotate", help="Retrieve annotations or labels."
)
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert,technology=rnaseq",
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
@click.option("--loglevel", type=LOGLEVEL_OPT, default="info", help="Logging level.")
@click.option(
    "--quiet", is_flag=True, default=False, help="No log or console output if applied."
)
def retrieve_tissues(
    terms, level, mode, fmt, metadata, filters, output, loglevel, quiet
):
    """Retrieval command for tissue ontology terms."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=loglevel, log_dir=get_log_dir()
    )

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


@retrieve_commands.command("diseases")
@click.option("--terms", type=str, default="MONDO:0004994,MONDO:0018177")
@click.option("--level", type=LEVEL_OPT, default="sample", help="GEO annotation level.")
@click.option(
    "--mode", type=MODE_OPT, default="direct", help="Mode to retrieve annotations."
)
@click.option("--fmt", type=FMT_OPT, default="parquet")
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert,technology=rnaseq",
    help="Filters for species, ecode, and technology. Run `metahq supported` for options.",
)
@click.option("--metadata", type=str, default="default")
@click.option(
    "--output",
    type=click.Path(),
    default="annotations.parquet",
    help="Path to outfile.",
)
@click.option("--loglevel", type=LOGLEVEL_OPT, default="info", help="Logging level.")
@click.option(
    "--quiet", is_flag=True, default=False, help="No log or console output if applied."
)
def retrieve_diseases(
    terms, level, mode, fmt, metadata, filters, output, loglevel, quiet
):
    """Retrieval command for disease ontology terms."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=loglevel, log_dir=get_log_dir()
    )

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
@click.option("--level", type=LEVEL_OPT, default="sample", help="GEO annotation level.")
@click.option("--fmt", type=FMT_OPT, default="parquet")
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert,technology=rnaseq",
    help="Filters for species, ecode, and technology. Run `metahq supported` for options.",
)
@click.option("--metadata", type=str, default="default")
@click.option(
    "--output",
    type=click.Path(),
    default="annotations.parquet",
    help="Path to outfile.",
)
@click.option("--loglevel", type=LOGLEVEL_OPT, default="info", help="Logging level.")
@click.option(
    "--quiet", is_flag=True, default=False, help="No log or console output if applied."
)
def retrieve_sex(terms, level, fmt, metadata, filters, output, loglevel, quiet):
    """Retrieval command for sex annotations."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=loglevel, log_dir=get_log_dir()
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


@retrieve_commands.command("age")
@click.option(
    "--terms",
    type=AGE_GROUP_OPT,
    default="all",
    help="Age groups to choose. Can combine like 'fetus,adult'.",
)
@click.option("--level", type=LEVEL_OPT, default="sample", help="GEO annotation level.")
@click.option(
    "--filters",
    type=str,
    default="species=human,ecode=expert,technology=rnaseq",
    help="Filters for species, ecode, and technology. Run `metahq supported` for options.",
)
@click.option("--metadata", type=str, default="default")
@click.option("--fmt", type=FMT_OPT, default="parquet")
@click.option(
    "--output",
    type=click.Path(),
    default="annotations.parquet",
    help="Path to outfile.",
)
@click.option("--loglevel", type=LOGLEVEL_OPT, default="info", help="Logging level.")
@click.option(
    "--quiet", is_flag=True, default=False, help="No log or console output if applied."
)
def retrieve_age(terms, level, fmt, metadata, filters, output, loglevel, quiet):
    """Retrieval command for age group annotations."""
    if metadata == "default":
        metadata = level

    verbose = set_verbosity(quiet)
    log = setup_logger(
        __name__, console=get_console(), level=loglevel, log_dir=get_log_dir()
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
