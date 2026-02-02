"""
CLI command to delete old version of MetaHQ
"""

import click
import os
import shutil
import sys
from metahq_core.util.progress import console
from pathlib import Path

from metahq_cli.logger import setup_logger
from metahq_cli.util.common_args import logging_args
from metahq_core.util.supported import get_default_log_dir
from metahq_cli.util.helpers import set_verbosity

from metahq_core.util.supported import get_config, get_config_file

@click.command(name="delete", context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-l",
    "--log-dir",
    type=click.Path(),
    default="default",
    help="Path to directory storing logs. Default is `/home/path/MetaHQ`.",
)
@click.option(
    "--all",
    "-a",
    is_flag=True,
    default=False,
    help="Will delete everything in MetaHQ directory",
)
@logging_args
def delete(log_level: str, log_dir: str, quiet: bool, all: bool):
    """Delete old version of MetaHQ"""
    if log_dir == "default":
        log_dir = str(get_default_log_dir())

    logger = setup_logger(__name__, level=log_level, log_dir=log_dir, console=console)
    verbose = set_verbosity(quiet)
    
    # read in config
    config = get_config()
    config_path = get_config_file()
    logger.info(f"The path to config file is found at: {config_path}")
    logger.info(f"Information in the config file is: {config}")
         
    if click.confirm(
        f"Delete MetaHQ database and config file or config directory?",
        default=False,
    ):
        logger.info("Deleting existing data directory...")
        shutil.rmtree(config["data_dir"])
        if all:
            logger.info("Deleting existing config directory...")
            shutil.rmtree(config_path.parent)
        else:
            logger.info("Deleting existing config file...")
            os.remove(config_path)
    else:
        logger.info("Keeping existing data directory and config file.")
        sys.exit("Terminating...")