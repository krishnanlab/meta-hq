"""
CLI command to validate data hasen't changed.
"""

import click
from metahq_core.util.progress import console
from pathlib import Path
import hashlib

from metahq_cli.logger import setup_logger
from metahq_cli.util.common_args import logging_args
from metahq_core.util.supported import get_default_log_dir
from metahq_cli.util.helpers import set_verbosity

from metahq_core.util.supported import get_config, get_config_file
from metahq_cli.util._validate import check_md5_match

@click.command(name="validate", context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-l",
    "--log-dir",
    type=click.Path(),
    default="default",
    help="Path to directory storing logs. Default is `/home/path/MetaHQ`.",
)
@logging_args
def validate(log_level: str, log_dir: str, quiet: bool):
    """Validate files are unchanged"""
    if log_dir == "default":
        log_dir = str(get_default_log_dir())

    logger = setup_logger(__name__, level=log_level, log_dir=log_dir, console=console)
    verbose = set_verbosity(quiet)
    
    # read in config
    config = get_config()
    config_path = get_config_file()
    logger.info(f"The path to config file is found at: {config_path}")
    logger.info(f"Validating the files for the config options: {config}")
    
    changed_files = check_md5_match(config["zenodo_doi"], config["data_dir"])
    
    if len(changed_files) == 0:
        logger.info("Validation Passed: All files validated to be correct")
    elif len(changed_files) > 0:
        logger.warning(
            f"Validation Failed: The following files are not the same as when downloaded. "
            "Run `metahq setup` again."
        )
        for afile in changed_files:
            logger.warning(afile)