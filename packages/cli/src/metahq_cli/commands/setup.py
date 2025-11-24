"""
Command to set up the meta-hq CLI.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-11-24 by Parker Hicks
"""

import click
from metahq_core.util.progress import console
from metahq_core.util.supported import (
    get_default_data_dir,
    get_default_log_dir,
    supported,
)

from metahq_cli.logger import setup_logger
from metahq_cli.setup.config import Config
from metahq_cli.setup.downloader import Downloader
from metahq_cli.util.helpers import set_verbosity
from metahq_cli.util.supported import LATEST_DATABASE

LOG_LEVEL_OPT = click.Choice(supported("log_levels"))


@click.command
@click.option("--doi", type=str, default="latest")
@click.option("--data_dir", type=click.Path(), default="default")
@click.option("--loglevel", type=LOG_LEVEL_OPT, default="info", help="Logging level.")
@click.option(
    "--logdir",
    type=str,
    default="default",
    help="Path to directory storing logs.",
)
@click.option(
    "--quiet", is_flag=True, default=False, help="No log or console output if applied."
)
def setup(doi: str, data_dir: str, loglevel: str, logdir: str, quiet: bool):
    """Creates the meta-hq package configuration file."""
    if logdir == "default":
        logdir = str(get_default_log_dir())

    logger = setup_logger(__name__, level=loglevel, log_dir=logdir, console=console)
    verbose = set_verbosity(quiet)

    if doi == "latest":
        doi = LATEST_DATABASE["doi"]

    if data_dir == "default":
        data_dir = str(get_default_data_dir())

    logger.info("Downloading MetaHQ database...")
    downloader = Downloader(
        doi, data_dir, logger=logger, logdir=logdir, verbose=verbose
    )
    downloader.get()
    downloader.extract()

    logger.info("Configuring MetaHQ...")
    config = Config(
        downloader.database_version,
        doi,
        data_dir,
        logdir,
        logger=logger,
        verbose=verbose,
    )
    config.setup()
