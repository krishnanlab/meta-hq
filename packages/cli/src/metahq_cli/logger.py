"""
Logger setup.

Author: Parker Hicks
Date: 2025-10-16

Last updated: 2026-02-04 by Parker Hicks
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from rich.logging import RichHandler

from metahq_cli.util.checkers import check_loglevel

if TYPE_CHECKING:
    from rich.console import Console


class ColoredFormatter(logging.Formatter):
    """Console color logger formatter."""

    COLORS = {
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold red",
    }

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, "white")
        record.levelname = f"[{color}][{levelname}][/{color}]"
        return super().format(record)


def setup_logger(
    name: str,
    console: Console,
    log_dir: str | Path,
    level: int | str = logging.INFO,
) -> logging.Logger:
    """Sets up a logger.

    Arguments:
        name (str):
            Logger name.

        log_dir (str | Path):
            Path to logging directory.

        level (int):
            Logging level.

    Returns:
        Configured logger.

    """
    logger = logging.getLogger(name)
    _level = check_loglevel(level)

    if logger.hasHandlers():
        return logger

    logger.setLevel(_level)

    # rich console handler
    console_handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        tracebacks_show_locals=True,
        markup=True,
        show_level=False,
        show_time=True,
        show_path=False,
        log_time_format="[%x %X]",
        omit_repeated_times=False,
    )

    console_formatter = ColoredFormatter(
        fmt="{levelname} {message}",
        style="{",
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # file handler
    file_handler = logging.FileHandler(
        Path(log_dir) / "log.log",
        encoding="utf-8",
    )
    # file formatter
    file_formatter = logging.Formatter(
        fmt="[{asctime}] [{levelname}] {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M",
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
