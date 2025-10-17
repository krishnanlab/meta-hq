"""
Logger setup.

Author: Parker Hicks
Date: 2025-10-16

Last updated: 2025-10-16 by Parker Hicks
"""

from __future__ import annotations

import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING

from metahq_core.util.supported import get_log_dir
from rich.logging import RichHandler

from metahq_cli.util.checkers import check_loglevel

if TYPE_CHECKING:
    from rich.console import Console

DEFAULT_LOGS = get_log_dir()


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
    level: int | str = logging.INFO,
    log_dir: str | Path = DEFAULT_LOGS,
) -> logging.Logger:
    """
    Sets up a logger.

    Parameters
    ----------
    name: str
        Logger name.

    log_dir: str | Path
        Path to logging directory. Default is ~/metahq/logs

    level: int
        Logging level.

    Returns
    -------
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
    date_time = datetime.now().strftime("%m-%d-%Y__%Hhr_%Mmin")
    file_handler = TimedRotatingFileHandler(
        Path(log_dir) / f"{date_time}.log",
        when="midnight",
        interval=1,
        backupCount=30,
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
