"""
Logger setup.

Author: Parker Hicks
Date: 2025-10-16

Last updated: 2026-02-04 by Parker Hicks
"""

import logging
from pathlib import Path


def setup_logger(
    name: str,
    log_dir: str | Path,
    level: int = logging.INFO,
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
    file_handler = logging.FileHandler(
        Path(log_dir) / "log.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
