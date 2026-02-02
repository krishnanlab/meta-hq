"""
CLI entry point for meta-hq.

Author: Parker Hicks
Date: 2025-09-05

Last updated: 2025-09-05
"""

import click

from metahq_cli.commands import retrieve_commands, search, setup, supported, validate, delete


@click.group()
def main():
    pass


main.add_command(setup)
main.add_command(search)
main.add_command(supported)
main.add_command(retrieve_commands, name="retrieve")
main.add_command(validate)
main.add_command(delete)

if __name__ == "__main__":
    main()
