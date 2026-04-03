"""
Gu et al. annotation processor.

Processes annotations from Gu et al. study, which provides tissue and
disease annotations for gene expression samples.
"""

import json
from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class GuProcessor(BaseProcessor):
    """
    Processor for Gu et al. annotations.

    Gu et al. provides tissue and disease annotations for gene expression samples.
    """

    source_name = "gu"
    version = "1.0.0"
    description = "Gu et al. tissue and disease annotations"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download Gu et al. data.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to Gu data file
        """
        data_file = kwargs.get("data_file", output_dir / "gu_etal__ids-GSM.bson")

        if not data_file.exists():
            # Try JSON alternative
            data_file = output_dir / "gu_etal__ids-GSM.json"

        if not data_file.exists():
            self.logger.warning(
                f"Gu et al. data file not found. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process Gu et al. annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to Gu data file (BSON or JSON)
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing Gu et al. annotations...")

        # Read Gu data
        with open(input_path, "r") as f:
            gu_data = json.load(f)

        records = []

        for sample_id, annotations in gu_data.items():
            if isinstance(annotations, dict):
                # Extract tissue annotations
                if "tissue" in annotations:
                    tissue_info = annotations["tissue"]
                    if isinstance(tissue_info, dict):
                        term_id = tissue_info.get("id", "na")
                        term_label = tissue_info.get("value", tissue_info.get("label", ""))

                        if term_label:
                            records.append(
                                {
                                    "sample_id": sample_id,
                                    "annotation_type": "tissue",
                                    "term_id": term_id,
                                    "term_label": term_label,
                                    "confidence": 0.8,
                                    "source": self.source_name,
                                }
                            )

                # Extract disease annotations
                if "disease" in annotations:
                    disease_info = annotations["disease"]
                    if isinstance(disease_info, dict):
                        term_id = disease_info.get("id", "na")
                        term_label = disease_info.get("value", disease_info.get("label", ""))

                        if term_label:
                            records.append(
                                {
                                    "sample_id": sample_id,
                                    "annotation_type": "disease",
                                    "term_id": term_id,
                                    "term_label": term_label,
                                    "confidence": 0.8,
                                    "source": self.source_name,
                                }
                            )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from Gu et al.")

        # Save processed data
        output_file = output_dir / "gu_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed Gu et al. data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)
        return True
