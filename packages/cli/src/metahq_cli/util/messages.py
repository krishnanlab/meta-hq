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
    sys.exit(1)


class TruncatedList:
    """Truncate long lists for a less egregious display."""

    def __init__(self, data, max_show=4):
        self.data: list[str] = data
        self.max_show: int = max_show

    def __repr__(self):
        per_side = self.max_show // 2

        start = ", ".join(map(str, self.data[:per_side]))
        end = ", ".join(map(str, self.data[-per_side:]))

        return f"[{start}, ... , {end}] (n={len(self.data)})"
