"""
Helper functions for CLI commands.

Author: Parker Hicks
Date: 2025-11-21

Last updated: 2025-11-21 by Parker Hicks
"""


def set_verbosity(quiet: bool):
    """Return the opposite of quiet."""
    if quiet:
        return False
    return True
