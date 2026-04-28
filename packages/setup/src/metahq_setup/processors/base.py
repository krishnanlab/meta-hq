"""
Base class for data source processors.

Defines the interface that all data source processors must implement
to ensure consistency across the pipeline.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    PROCESSED_DIR,
)
from metahq_setup.util.logging import setup_logger


class ProcessorError(Exception):
    """Exception raised when processor encounters an error."""

    pass


class ValidationError(Exception):
    """Exception raised when processor validation fails."""

    pass


class BaseProcessor(ABC):
    """
    Abstract base class for all data source processors.

    All data source processors must inherit from this class and implement
    the required methods. This ensures a consistent interface across all
    processors in the pipeline.

    Attributes:
        source_name (str):
            Unique identifier for this data source (e.g., "gemma", "ale")
        version (str):
            Version string for this processor
        description (str):
            Human-readable description of the data source
        logger (logging.Logger):
            Logger instance for this processor
    """

    # Class attributes that subclasses must define
    source_name: str
    version: str
    description: str = ""

    def __init__(self):
        """Initialize the base processor."""
        # Validate that required class attributes are defined
        if not hasattr(self, "source_name") or not self.source_name:
            raise NotImplementedError("Processor must define source_name")
        if not hasattr(self, "version") or not self.version:
            raise NotImplementedError("Processor must define version")

        # Set up logger
        self.logger = setup_logger(f"metahq_setup.processors.{self.source_name}")

    @abstractmethod
    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process raw data into standardized annotation format.

        The output DataFrame must have the following columns:
        - accession: str - Sample or study identifier (GSM, GSE, SRR, etc.)
        - attribute: str - Type of annotation (tissue, disease, cell_type, sex, age)
        - term_id: str - Ontology term ID (e.g., MONDO:0004994, UBERON:0000948)
        - term_name: str - Human-readable term label
        - ecode: str - Evidence code (expert, semi, crowd, automated)

        Arguments:
            output_dir (Path):
                Directory to write processed output
            **kwargs:
                Additional processor-specific arguments

        Returns:
            (pl.DataFrame): Standardized annotations DataFrame

        Raises:
            ProcessorError: If processing fails
        """
        pass

    @abstractmethod
    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate that processed data meets requirements.

        Checks that the DataFrame has the required columns and
        that values are in the expected format.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate

        Returns:
            (bool): True if validation passes

        Raises:
            ValidationError: If validation fails
        """
        pass

    def cleanup(self, temp_dir: Path) -> None:
        """
        Clean up temporary files after processing.

        Arguments:
            temp_dir (Path):
                Directory containing temporary files to clean
        """
        if temp_dir.exists():
            import shutil

            shutil.rmtree(temp_dir)
            self.logger.info(f"Cleaned up temporary directory: {temp_dir}")

    def run(
        self,
        output_dir: Path = PROCESSED_DIR,
        validate_output: bool = True,
        **kwargs,
    ) -> pl.DataFrame:
        """
        Run the complete processor workflow: process, validate.

        Arguments:
            output_dir (Path):
                Directory for outputs
            validate_output (bool):
                Whether to validate processed data
            **kwargs:
                Additional processor-specific arguments

        Returns:
            (pl.DataFrame): Processed and validated annotations

        Raises:
            ProcessorError: If any step fails
            ValidationError: If validation fails
        """
        self.logger.info(f"Starting {self.source_name} processor (v{self.version})")

        # Process
        self.logger.info("Processing data...")
        data = self.process(output_dir, **kwargs)

        # Sort
        data = data.sort([COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID])

        # Validate
        if validate_output:
            self.logger.info("Validating output...")
            self.validate(data)

        self.logger.info(
            f"Completed {self.source_name} processor. Produced {len(data)} annotations."
        )
        return data

    def _validate_required_columns(self, data: pl.DataFrame) -> None:
        """
        Validate that DataFrame has required columns.

        Arguments:
            data (pl.DataFrame):
                DataFrame to validate

        Raises:
            ValidationError: If required columns are missing
        """
        required_columns = [
            COL_ACCESSION,
            COL_ATTRIBUTE,
            COL_TERM_ID,
            COL_TERM_NAME,
            COL_ECODE,
        ]

        missing_columns = [col for col in required_columns if col not in data.columns]

        if missing_columns:
            raise ValidationError(
                f"Missing required columns: {missing_columns}. "
                f"DataFrame has: {data.columns}"
            )

    def __repr__(self) -> str:
        """String representation of processor."""
        return f"{self.__class__.__name__}(source={self.source_name}, version={self.version})"
