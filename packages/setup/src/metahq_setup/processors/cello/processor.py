"""
CellO cell type annotation processor.

Processes cell type annotations from CellO, which provides automated
cell type predictions for bulk RNA-seq samples.
"""

import json
from pathlib import Path

import polars as pl

from metahq_setup.processors.base import BaseProcessor, ProcessorError
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class CellOProcessor(BaseProcessor):
    """
    Processor for CellO cell type annotations.

    CellO provides automated cell type predictions using the Cell Ontology (CL).
    """

    source_name = "cello"
    version = "1.0.0"
    description = "CellO cell type annotations for bulk RNA-seq"

    def download(self, output_dir: Path, **kwargs) -> Path:
        """
        Download CellO data.

        Note: CellO data is typically pre-processed from CellO predictions.

        Arguments:
            output_dir (Path):
                Directory to save data
            **kwargs:
                Additional arguments

        Returns:
            (Path): Path to CellO data file
        """
        data_file = kwargs.get("data_file", output_dir / "cello_bulk_labels.json")

        if not data_file.exists():
            self.logger.warning(
                f"CellO data file not found: {data_file}. "
                "Please provide the file path via data_file kwarg."
            )

        return data_file

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process CellO annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to CellO JSON file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing CellO annotations...")

        # Read CellO data
        with open(input_path, "r") as f:
            cello_data = json.load(f)

        records = []

        for sample_id, annotations in cello_data.items():
            if isinstance(annotations, dict):
                # Extract cell type predictions
                cell_types = annotations.get("cell_types", [])
                if isinstance(cell_types, list):
                    for cell_type in cell_types:
                        if isinstance(cell_type, dict):
                            term_id = cell_type.get("term_id", "")
                            term_label = cell_type.get("term_label", "")
                            confidence = cell_type.get("confidence", 0.5)

                            if term_id and term_label:
                                records.append(
                                    {
                                        "sample_id": sample_id,
                                        "annotation_type": "cell_type",
                                        "term_id": term_id,
                                        "term_label": term_label,
                                        "confidence": float(confidence),
                                        "source": self.source_name,
                                    }
                                )

        result_df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(result_df)} annotations from CellO")

        # Save processed data
        output_file = output_dir / "cello_processed.parquet"
        result_df.write_parquet(output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed CellO data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)

        # Check that all annotations are cell_type
        types = data["annotation_type"].unique().to_list()
        if types != ["cell_type"]:
            self.logger.warning(f"Expected only cell_type annotations, got: {types}")

        return True
