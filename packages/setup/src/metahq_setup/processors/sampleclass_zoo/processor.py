"""
SampleClass Zoo annotation processor.

Processes annotations from the SampleClass Zoo study, which provides
tissue and disease classifications for gene expression samples.
"""

from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class SampleClassZooProcessor(BaseProcessor):
    """
    Processor for SampleClass Zoo annotations.

    SampleClass Zoo provides tissue and disease classifications.
    """

    source_name = "sampleclass_zoo"
    version = "1.0.0"
    description = "SampleClass Zoo tissue and disease classifications"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download SampleClass Zoo data.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to SampleClass Zoo data file
        """
        data_file = kwargs.get(
            "data_file",
            output_dir / "samp-class-zoo__tissue-disease_annotations.parquet",
        )

        if not data_file.exists():
            self.logger.warning(
                f"SampleClass Zoo data file not found: {data_file}. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process SampleClass Zoo annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to SampleClass Zoo parquet file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing SampleClass Zoo annotations...")

        # Read data
        df = pl.read_parquet(input_path)

        records = []

        for row in df.iter_rows(named=True):
            sample_id = row.get("sample_id", row.get("GSM", ""))

            if not sample_id:
                continue

            # Extract tissue annotations
            if "tissue" in row and row["tissue"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "tissue",
                        "term_id": row.get("tissue_id", "na"),
                        "term_label": row["tissue"],
                        "confidence": 0.75,
                        "source": self.source_name,
                    }
                )

            # Extract disease annotations
            if "disease" in row and row["disease"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "disease",
                        "term_id": row.get("disease_id", "na"),
                        "term_label": row["disease"],
                        "confidence": 0.75,
                        "source": self.source_name,
                    }
                )

        result_df = pl.DataFrame(records)
        self.logger.info(
            f"Processed {len(result_df)} annotations from SampleClass Zoo"
        )

        # Save processed data
        output_file = output_dir / "sampleclass_zoo_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed SampleClass Zoo data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)
        return True
