"""
CREEDS perturbation annotation processor.

Processes annotations from CREEDS (CRowd Extracted Expression of Differential Signatures),
which provides disease and drug perturbation annotations.
"""

from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class CREEDSProcessor(BaseProcessor):
    """
    Processor for CREEDS perturbation annotations.

    CREEDS provides crowd-sourced annotations for disease and drug perturbations.
    """

    source_name = "creeds"
    version = "1.0.0"
    description = "CREEDS crowd-sourced perturbation annotations"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download CREEDS data.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to CREEDS data file
        """
        data_file = kwargs.get("data_file", output_dir / "creeds.parquet")

        if not data_file.exists():
            self.logger.warning(
                f"CREEDS data file not found: {data_file}. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process CREEDS annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to CREEDS parquet file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing CREEDS annotations...")

        # Read CREEDS data
        df = pl.read_parquet(input_path)

        records = []

        for row in df.iter_rows(named=True):
            sample_id = row.get("sample_id", row.get("GSM", ""))

            if not sample_id:
                continue

            # Extract disease annotations if present
            if "disease" in row and row["disease"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "disease",
                        "term_id": row.get("disease_id", "na"),
                        "term_label": row["disease"],
                        "confidence": 0.7,  # CREEDS is crowd-sourced
                        "source": self.source_name,
                    }
                )

            # Extract tissue annotations if present
            if "tissue" in row and row["tissue"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "tissue",
                        "term_id": row.get("tissue_id", "na"),
                        "term_label": row["tissue"],
                        "confidence": 0.7,
                        "source": self.source_name,
                    }
                )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from CREEDS")

        # Save processed data
        output_file = output_dir / "creeds_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed CREEDS data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)
        return True
