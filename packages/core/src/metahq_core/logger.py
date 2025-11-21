"""
Logger setup.

Author: Parker Hicks
Date: 2025-10-16

Last updated: 2025-11-20 by Parker Hicks
"""

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from metahq_core.util.supported import get_log_dir

DEFAULT_LOGS = get_log_file()


def setup_logger(
    name: str, level: int = logging.INFO, log_dir: str | Path = DEFAULT_LOGS
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

    if logger.hasHandlers():
        return logger

    logger.setLevel(level)

    # formatter
    formatter = logging.Formatter(
        fmt="[{asctime}] [{levelname}] {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M",
    )

    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # file handler
    file_handler = TimedRotatingFileHandler(
        Path(log_dir) / "log.log",
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
