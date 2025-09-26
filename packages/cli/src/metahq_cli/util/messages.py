"""
Templates for CLI errors and warnings.

Author: Parker Hicks
Date: 2025-09-25

Last updated: 2025-09-25
"""

import sys

import click


def warning(message):
    click.secho(f"WARNING: {message}", fg="yellow")


def error(message):
    click.secho(f"ERROR: {message}", fg="red")

    click.echo("Exiting...")
    sys.exit(1)
