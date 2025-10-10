"""
Rich progress displays.

Author: Parker Hicks
Date: 2025-09-25

Last updated: 2025-09-26 by Parker Hicks
"""

from functools import wraps

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

console = Console()


def spinner(desc, p_message, end_message):
    """Function decorator to apply a rich progress spinner."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with Progress(
                SpinnerColumn(speed=2),
                TextColumn(desc),
                console=console,
            ) as progress:
                task = progress.add_task(p_message, total=None)

                result = func(*args, **kwargs)
                progress.update(task, description=end_message)

                return result

        return wrapper

    return decorator


def progress_bar():
    """
    Creates a custom rich progress bar.

    Taken from Timothy Gebhard:
    https://timothygebhard.de/posts/richer-progress-bars-for-rich/

    """
    return Progress(
        TextColumn("{task.description} [progress.percentage]{task.percentage:>3.0f}%"),
        BarColumn(),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=console,
    )


def progress_wrapper(desc, verbose, total, func, *args, **kwargs):
    """Function wrapper to apply a spinner while process ongoing."""
    if verbose:
        with Progress(
            SpinnerColumn(speed=2),
            TextColumn(desc),
            console=console,
        ) as progress:
            task = progress.add_task(desc, total=total)

            result = func(*args, **kwargs)
            progress.update(task, description=desc)

            return result

    return func(*args, **kwargs)
