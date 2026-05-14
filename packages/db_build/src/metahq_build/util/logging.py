"""
Logging configuration for metahq-build.

Provides structured logging with multiple handlers for console and file output.
Supports different log levels and formatting options.
"""

import logging
import sys
from pathlib import Path

from rich.logging import RichHandler


def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Path | None = None,
    use_rich: bool = True,
) -> logging.Logger:
    """
    Configure and return a logger with console and optional file handlers.

    Arguments:
        name (str):
            Name of the logger (typically __name__)
        level (int):
            Logging level (e.g., logging.INFO, logging.DEBUG)
        log_file (Path | None):
            Path to log file. If None, no file handler is added
        use_rich (bool):
            Whether to use rich console handler for colored output

    Returns:
        (logging.Logger): Configured logger instance

    Examples:
        >>> logger = setup_logger(__name__, level=logging.DEBUG)
        >>> logger.info("Processing started")
        >>> logger.warning("Missing data for sample GSM123")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Console handler
    if use_rich:
        console_handler = RichHandler(
            rich_tracebacks=True,
            markup=True,
            show_time=True,
            show_path=True,
        )
        console_format = "%(message)s"
    else:
        console_handler = logging.StreamHandler(sys.stdout)
        console_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    console_handler.setLevel(level)
    console_formatter = logging.Formatter(console_format)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_format = "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        file_formatter = logging.Formatter(file_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # Prevent propagation to root logger
    logger.propagate = False

    return logger


class PipelineLogger:
    """
    Specialized logger for pipeline execution with stage tracking.

    Provides methods for logging pipeline-specific events like stage
    transitions, progress updates, and data statistics.

    Attributes:
        logger (logging.Logger):
            Underlying logger instance
        current_stage (str | None):
            Name of the current pipeline stage

    Examples:
        >>> pipeline_logger = PipelineLogger("metahq_build.pipeline")
        >>> pipeline_logger.start_stage("fetch_metadata")
        >>> pipeline_logger.log_stat("Fetched samples", 1500)
        >>> pipeline_logger.end_stage("fetch_metadata")
    """

    def __init__(
        self,
        name: str,
        level: int = logging.INFO,
        log_file: Path | None = None,
    ):
        self.logger = setup_logger(name, level=level, log_file=log_file)
        self.current_stage: str | None = None

    def start_stage(self, stage_name: str) -> None:
        """
        Log the start of a pipeline stage.

        Arguments:
            stage_name (str):
                Name of the pipeline stage
        """
        self.current_stage = stage_name
        self.logger.info("=" * 80)
        self.logger.info(f"Starting stage: {stage_name}")
        self.logger.info("=" * 80)

    def end_stage(self, stage_name: str) -> None:
        """
        Log the completion of a pipeline stage.

        Arguments:
            stage_name (str):
                Name of the pipeline stage
        """
        self.logger.info(f"Completed stage: {stage_name}")
        self.logger.info("")
        self.current_stage = None

    def log_stat(self, description: str, value: int | float | str) -> None:
        """
        Log a statistic with description and value.

        Arguments:
            description (str):
                Description of the statistic
            value (int | float | str):
                Value of the statistic
        """
        self.logger.info(f"  {description}: {value}")

    def log_progress(self, message: str) -> None:
        """
        Log a progress message within a stage.

        Arguments:
            message (str):
                Progress message to log
        """
        prefix = f"[{self.current_stage}] " if self.current_stage else ""
        self.logger.info(f"{prefix}{message}")

    def log_error(self, message: str, exc_info: bool = False) -> None:
        """
        Log an error message.

        Arguments:
            message (str):
                Error message to log
            exc_info (bool):
                Whether to include exception traceback
        """
        self.logger.error(message, exc_info=exc_info)

    def log_warning(self, message: str) -> None:
        """
        Log a warning message.

        Arguments:
            message (str):
                Warning message to log
        """
        self.logger.warning(message)

    def log_debug(self, message: str) -> None:
        """
        Log a debug message.

        Arguments:
            message (str):
                Debug message to log
        """
        self.logger.debug(message)


def get_default_log_file(package_root: Path) -> Path:
    """
    Get default log file path for the package.

    Arguments:
        package_root (Path):
            Root directory of the metahq_build package

    Returns:
        (Path): Path to default log file in .log directory
    """
    log_dir = package_root / ".log"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "metahq_build.log"
