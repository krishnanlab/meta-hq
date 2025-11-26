"""
Command to set up the meta-hq CLI.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-11-26 by Parker Hicks
"""

from pathlib import Path

import click
from metahq_core.util.progress import console
from metahq_core.util.supported import get_default_data_dir, get_default_log_dir

from metahq_cli.logger import setup_logger
from metahq_cli.setup.config import Config
from metahq_cli.setup.downloader import Downloader
from metahq_cli.util.common_args import logging_args
from metahq_cli.util.helpers import set_verbosity
from metahq_cli.util.supported import LATEST_DATABASE


@click.command
@click.option(
    "--doi",
    type=str,
    default="latest",
    help="Zenodo DOI of the MetaHQ database. Default is `latest`.",
)
@click.option(
    "--data-dir",
    type=click.Path(),
    default="default",
    help="Path to directory to store the database. Default is `/home/path/.metahq_data`.",
)
@click.option(
    "--log-dir",
    type=click.Path(),
    default="default",
    help="Path to directory storing logs. Default is `/home/path/MetaHQ`.",
)
@logging_args
def setup(doi: str, data_dir: str, log_level: str, log_dir: str, quiet: bool):
    """Downloads the MetahQ database and configures the CLI."""
    if log_dir == "default":
        log_dir = str(get_default_log_dir())

    logger = setup_logger(__name__, level=log_level, log_dir=log_dir, console=console)
    verbose = set_verbosity(quiet)

    if doi == "latest":
        doi = LATEST_DATABASE["doi"]

    if data_dir == "default":
        data_dir = str(get_default_data_dir())

    logger.info("Downloading MetaHQ database...")
    downloader = Downloader(
        doi, data_dir, logger=logger, logdir=str(log_dir), verbose=verbose
    )
    downloader.get()
    downloader.extract()

    logger.info("Configuring MetaHQ...")
    config = Config(
        downloader.database_version,
        doi,
        data_dir,
        log_dir,
        logger=logger,
        verbose=verbose,
    )
    config.setup()
