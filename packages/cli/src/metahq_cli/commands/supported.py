"""
MetaHQ CLI command to display supported options for retrieval arguments.

Author: Parker Hicks
Date: 2025-09-29

Last updated: 2025-09-29 by Parker Hicks
"""

import click
from metahq_core.util.supported import _supported
from rich.console import Console
from rich.table import Table

console = Console()


@click.command
def supported():
    """Display all supported entities and their options."""
    table = Table(title="MetaHQ Supported Entities")
    table.add_column("Entity", style="cyan")
    table.add_column("Available", style="green")

    __supported = _supported()
    for i, (entity, avail) in enumerate(__supported.items()):
        avail = ", ".join(avail).strip()
        entity = " ".join(entity.split("_")).strip()
        table.add_row(entity, avail)
        if i < len(__supported) - 1:  # Don't add separator after last row
            table.add_section()

    console.print(table)
