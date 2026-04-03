"""
Golightly sex annotation processor.

Processes sex predictions from Golightly et al. study, which provides
automated sex predictions for gene expression samples.
"""

from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class GolightlyProcessor(BaseProcessor):
    """
    Processor for Golightly sex annotations.

    Golightly provides automated sex predictions based on gene expression patterns.
    """

    source_name = "golightly"
    version = "1.0.0"
    description = "Golightly automated sex predictions"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download Golightly data.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to Golightly data file
        """
        data_file = kwargs.get("data_file", output_dir / "golightly.parquet")

        if not data_file.exists():
            self.logger.warning(
                f"Golightly data file not found: {data_file}. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process Golightly annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to Golightly parquet file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing Golightly annotations...")

        # Read Golightly data
        df = pl.read_parquet(input_path)

        records = []

        for row in df.iter_rows(named=True):
            sample_id = row.get("sample_id", row.get("GSM", ""))
            sex = row.get("sex", row.get("predicted_sex", ""))
            confidence = row.get("confidence", 0.8)

            if sample_id and sex and sex not in ["na", "", "unknown"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "sex",
                        "term_id": "na",
                        "term_label": sex.lower(),
                        "confidence": float(confidence),
                        "source": self.source_name,
                    }
                )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from Golightly")

        # Save processed data
        output_file = output_dir / "golightly_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed Golightly data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)

        # Check that all annotations are sex
        types = data["annotation_type"].unique().to_list()
        if types != ["sex"]:
            self.logger.warning(f"Expected only sex annotations, got: {types}")

        return True
