"""
Command to set up the meta-hq CLI.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-05
"""

from importlib.metadata import version

import click
from metahq_core.setup import Config

METAHQ_VERSION = version("metahq_core")


@click.command
@click.option("--zenodo_doi", type=str, default="latest")
@click.option("--data_dir", type=click.Path(), default="default")
def setup(zenodo_doi: str, data_dir: str):
    """Creates the meta-hq package configuration file."""
    config = Config(METAHQ_VERSION, zenodo_doi, data_dir)
    config.check()
    config.setup()
    click.echo(f"Config saved to {config.path}.")
