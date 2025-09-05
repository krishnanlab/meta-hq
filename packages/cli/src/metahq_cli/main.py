"""
CLI entry point for meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-05
"""

import click

from metahq_cli.commands import retrieve_commands, setup


@click.group()
def main():
    pass


main.add_command(setup)
main.add_command(retrieve_commands, name="retrieve")

if __name__ == "__main__":
    main()
