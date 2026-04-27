"""
URSA tissue annotation processor.

Processes expert-curated tissue annotations from URSA, which provides
UBERON/CL term IDs for GEO samples.
"""

from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    UBERON_OBO,
    UBERON_SYSTEMS,
    URSA_CSV,
)
from metahq_setup.ontology import Ontology, get_system_descendants
from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class URSAProcessor(BaseProcessor):
    """
    Processor for URSA tissue annotations.

    URSA provides expert-curated UBERON/CL tissue term IDs for GEO samples.
    The source file is a headerless CSV with columns: sample_id, study_id,
    term_id.
    """

    source_name = "ursa"
    version = "1.0.0"
    description = "URSA expert-curated tissue annotations"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process the URSA CSV into standardized sample annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path | str) — override the default URSA CSV
                input path (defaults to ``URSA_CSV`` from config).

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", URSA_CSV))
        self.logger.info("Processing URSA CSV file: %s", input_path)

        df = pl.read_csv(
            input_path,
            has_header=False,
            new_columns=["sample_id", "study_id", "term_id"],
            schema={"sample_id": pl.String, "study_id": pl.String, "term_id": pl.String},
        )

        self.logger.info("Read %s rows from URSA CSV.", df.height)

        # Map term IDs to human-readable names via UBERON OBO.
        self.logger.info("Loading UBERON ontology for ID -> name mapping: %s", UBERON_OBO)
        class_dict = Ontology.from_obo(UBERON_OBO).class_dict

        df = df.with_columns(
            pl.col("term_id").replace(class_dict, default="NA").alias("term_label")
        )

        unmapped = df.filter(pl.col("term_label") == "NA").height
        if unmapped > 0:
            self.logger.warning(
                "%s rows could not be mapped to a term name and will be dropped.", unmapped
            )
        df = df.filter(pl.col("term_label") != "NA")

        # Filter to descendants of UBERON/CL system-level terms.
        self.logger.info("Loading UBERON system descendants for filtering...")
        valid_terms = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)
        before = df.height
        df = df.filter(pl.col("term_id").is_in(valid_terms))
        self.logger.info(
            "Filtered annotations from %s to %s using UBERON/CL system descendants.",
            before,
            df.height,
        )

        result_df = df.select(
            pl.col("sample_id"),
            pl.lit("tissue").alias("annotation_type"),
            pl.col("term_id"),
            pl.col("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info(
            "Produced %s tissue annotations across %s unique samples.",
            result_df.height,
            result_df["sample_id"].n_unique(),
        )

        result_df = result_df.rename({
            "sample_id": COL_ACCESSION,
            "annotation_type": COL_ATTRIBUTE,
            "term_label": COL_TERM_NAME,
        })

        output_file = output_dir / "ursa_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed URSA data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        has_tissue = "tissue" in data[COL_ATTRIBUTE].unique().to_list()
        if not has_tissue:
            self.logger.warning("No tissue annotations found in URSA output.")

        return True
