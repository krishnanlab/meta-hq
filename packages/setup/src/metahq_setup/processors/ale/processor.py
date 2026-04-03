"""
ALE (Giles et al.) annotation processor.

Processes manually curated annotations from the ALE study by Giles et al.
including tissue, age, and sex annotations.
"""

from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor, ProcessorError
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class ALEProcessor(BaseProcessor):
    """
    Processor for ALE (Giles et al.) manual annotations.

    ALE provides expert-curated annotations for GEO samples including
    tissue types, age, and sex information.
    """

    source_name = "ale"
    version = "1.0.0"
    description = "ALE (Giles et al.) manually curated annotations"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download ALE data.

        Note: ALE data is typically pre-downloaded as it's a static dataset.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to ALE TSV file
        """
        # ALE is a static dataset, typically already present
        # In a real implementation, this would download from a known URL
        data_file = kwargs.get("data_file", output_dir / "ALE-giles_etal-geo_manual_labels_jdw.tsv")

        if not data_file.exists():
            self.logger.warning(
                f"ALE data file not found: {data_file}. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process ALE annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to ALE TSV file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments (bto_uberon_map for tissue mapping)

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing ALE annotations...")

        # Read ALE data
        df = pl.read_csv(
            input_path,
            separator="\t",
            skip_rows=1,
            new_columns=["GSM", "brenda", "age_months", "gender"],
            schema={
                "GSM": pl.String,
                "brenda": pl.String,
                "age_months": pl.Float64,
                "gender": pl.String,
            },
        ).unique()

        # Process into standardized format
        records = []

        for row in df.iter_rows(named=True):
            gsm = row["GSM"]

            # Add tissue annotation (BTO term)
            if row["brenda"] and row["brenda"] != "na":
                records.append(
                    {
                        "sample_id": gsm,
                        "annotation_type": "tissue",
                        "term_id": row["brenda"],  # BTO term
                        "term_label": row["brenda"],
                        "confidence": 1.0,  # Expert curated
                        "source": self.source_name,
                    }
                )

            # Add sex annotation
            if row["gender"] and row["gender"] not in ["na", ""]:
                records.append(
                    {
                        "sample_id": gsm,
                        "annotation_type": "sex",
                        "term_id": "na",
                        "term_label": row["gender"],
                        "confidence": 1.0,
                        "source": self.source_name,
                    }
                )

            # Add age annotation
            if row["age_months"] is not None:
                age_years = row["age_months"] / 12
                records.append(
                    {
                        "sample_id": gsm,
                        "annotation_type": "age",
                        "term_id": "na",
                        "term_label": f"{age_years:.1f} years",
                        "confidence": 1.0,
                        "source": self.source_name,
                    }
                )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from ALE")

        # Save processed data
        output_file = output_dir / "ale_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed ALE data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)

        # Check that we have tissue annotations
        has_tissue = "tissue" in data["annotation_type"].unique().to_list()
        if not has_tissue:
            self.logger.warning("No tissue annotations found in ALE data")

        return True
