"""
URSA annotation processor.

Processes annotations from URSA (Uniform RNA-Seq Archive), which provides
tissue annotations for gene expression samples.
"""

from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class URSAProcessor(BaseProcessor):
    """
    Processor for URSA annotations.

    URSA provides tissue annotations for RNA-seq samples.
    """

    source_name = "ursa"
    version = "1.0.0"
    description = "URSA tissue annotations for RNA-seq samples"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download URSA data.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to URSA data file
        """
        data_file = kwargs.get("data_file", output_dir / "ursa_processed.tsv")

        if not data_file.exists():
            # Try parquet alternative
            data_file = output_dir / "ursahd.parquet"

        if not data_file.exists():
            self.logger.warning(
                f"URSA data file not found. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process URSA annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to URSA data file (TSV or Parquet)
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing URSA annotations...")

        # Read URSA data (handle both TSV and Parquet)
        if input_path.suffix == ".parquet":
            df = pl.read_parquet(input_path)
        else:
            df = pl.read_csv(input_path, separator="\t")

        records = []

        for row in df.iter_rows(named=True):
            sample_id = row.get("sample_id", row.get("SRR", row.get("SRS", "")))

            if not sample_id:
                continue

            # Extract tissue annotations
            tissue_id = row.get("tissue_id", row.get("uberon_id", "na"))
            tissue_label = row.get("tissue", row.get("tissue_name", ""))

            if tissue_label and tissue_label not in ["na", "", "unknown"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "tissue",
                        "term_id": tissue_id,
                        "term_label": tissue_label,
                        "confidence": 0.85,
                        "source": self.source_name,
                    }
                )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from URSA")

        # Save processed data
        output_file = output_dir / "ursa_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed URSA data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)

        # Check that all annotations are tissue
        types = data["annotation_type"].unique().to_list()
        if types != ["tissue"]:
            self.logger.warning(f"Expected only tissue annotations, got: {types}")

        return True
