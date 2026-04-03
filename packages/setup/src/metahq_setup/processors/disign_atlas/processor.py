"""
DiSignAtlas annotation processor.

Processes annotations from DiSignAtlas, which provides disease signatures
and tissue annotations for gene expression studies.
"""

from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class DiSignAtlasProcessor(BaseProcessor):
    """
    Processor for DiSignAtlas annotations.

    DiSignAtlas provides disease signatures and tissue annotations.
    """

    source_name = "disign_atlas"
    version = "1.0.0"
    description = "DiSignAtlas disease and tissue annotations"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download DiSignAtlas data.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to DiSignAtlas data file
        """
        data_file = kwargs.get("data_file", output_dir / "DiSignAtlas_annotations.tsv")

        if not data_file.exists():
            self.logger.warning(
                "DiSignAtlas data file not found: %s. "
                "Please provide the file path via data_file kwarg.",
                data_file,
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process DiSignAtlas annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to DiSignAtlas TSV file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing DiSignAtlas annotations...")

        # Read DiSignAtlas data
        df = pl.read_csv(input_path, separator="\t")

        records = []

        for row in df.iter_rows(named=True):
            sample_id = row.get("sample_id", row.get("GSM", ""))

            if not sample_id:
                continue

            # Extract disease annotations
            disease = row.get("disease", row.get("condition", ""))
            disease_id = row.get("disease_id", row.get("mondo_id", "na"))

            if disease and disease not in ["na", "", "normal", "healthy"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "disease",
                        "term_id": disease_id,
                        "term_label": disease,
                        "confidence": 0.8,
                        "source": self.source_name,
                    }
                )

            # Extract tissue annotations
            tissue = row.get("tissue", "")
            tissue_id = row.get("tissue_id", row.get("uberon_id", "na"))

            if tissue and tissue not in ["na", "", "unknown"]:
                records.append(
                    {
                        "sample_id": sample_id,
                        "annotation_type": "tissue",
                        "term_id": tissue_id,
                        "term_label": tissue,
                        "confidence": 0.8,
                        "source": self.source_name,
                    }
                )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from DiSignAtlas")

        # Save processed data
        output_file = output_dir / "disign_atlas_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed DiSignAtlas data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)
        return True
