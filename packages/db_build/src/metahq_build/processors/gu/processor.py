"""
Gu 2023 annotation processor.

Processes expert-curated tissue and disease annotations from Gu et al. 2023,
which provides manual annotations for SRA samples.
"""

from pathlib import Path

import polars as pl

from metahq_build.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    ECODE_EXPERT,
    GU_2023_CSV,
    GU_DISEASE_MONDO,
    GU_TISSUE_UBERON,
    MONDO_OBO,
    MONDO_SYSTEMS,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_build.ontology import get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class GuProcessor(BaseProcessor):
    """
    Processor for Gu 2023 expert-curated annotations.

    Provides manually curated tissue and disease annotations for SRA samples.
    """

    source_name = "Gu_2023"
    version = "1.0.0"
    description = "Gu et al. tissue and disease annotations"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """Process Gu 2023 annotations into standardized format.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path) - override Gu 2023 CSV input file

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", GU_2023_CSV))
        self.logger.info("Processing Gu 2023 annotations from %s", input_path)

        # Load data, skipping first header line
        df = (
            pl.read_csv(input_path, skip_rows=1, null_values=["NA", "na"])
            .select(
                [
                    "Sample ID",
                    "Manual annotation",
                    "Sample info #manual annotation",
                ]
            )
            .rename(
                {
                    "Sample ID": COL_ACCESSION,
                    "Manual annotation": "tissue_name",
                    "Sample info #manual annotation": "disease_name",
                }
            )
            # Filter to rows with at least one annotation (tissue or disease)
            .filter(
                ~pl.all_horizontal(pl.col(["tissue_name", "disease_name"]).is_null())
            )
        )

        self.logger.info("Loaded %s samples with annotations", df.height)

        # Process disease annotations
        disease_records = self._process_disease(df)

        # Process tissue annotations
        tissue_records = self._process_tissue(df)

        # Combine all annotation types
        result_df = pl.concat([disease_records, tissue_records], how="vertical").sort(
            [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME]
        )

        self.logger.info(
            "Produced %s total annotations (%s disease, %s tissue)",
            result_df.height,
            disease_records.height,
            tissue_records.height,
        )

        # Save processed data
        output_file = output_dir / "gu_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def _process_disease(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process disease annotations.

        Arguments:
            df (pl.DataFrame):
                Data with 'sample_id' and 'disease_name' columns.

        Returns:
            (pl.DataFrame): Disease annotation records.
        """
        # Load disease name → MONDO mapping
        disease_map = pl.read_csv(GU_DISEASE_MONDO).rename(
            {"disease_name": "disease_name"}
        )

        # Join with mapping
        mapped = (
            df.filter(pl.col("disease_name").is_not_null())
            .select([COL_ACCESSION, "disease_name"])
            .join(disease_map, on="disease_name", how="left")
            .filter(pl.col("mondo_id").is_not_null() & (pl.col("mondo_id") != "na"))
        )

        # Load MONDO system descendants for filtering
        self.logger.info("Loading MONDO system descendants for disease filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)

        # Filter to valid MONDO descendants
        before = mapped.height
        mapped_filtered = mapped.filter(pl.col("mondo_id").is_in(valid_mondo))

        self.logger.info(
            "Filtered disease from %s to %s rows using MONDO system descendants.",
            before,
            mapped_filtered.height,
        )

        # Create disease annotation records
        disease_records = mapped_filtered.select(
            pl.col(COL_ACCESSION),
            pl.lit("disease").alias(COL_ATTRIBUTE),
            pl.col("mondo_id").alias(COL_TERM_ID),
            pl.col("disease_name").alias(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        self.logger.info("Processed %s disease annotations", disease_records.height)
        return disease_records

    def _process_tissue(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process tissue annotations.

        Arguments:
            df (pl.DataFrame):
                Data with 'sample_id' and 'tissue_name' columns.

        Returns:
            (pl.DataFrame): Tissue annotation records.
        """
        # Load tissue name → UBERON mapping
        tissue_map = pl.read_csv(GU_TISSUE_UBERON).rename(
            {"tissue_name": "tissue_name"}
        )

        # Join with mapping
        mapped = (
            df.filter(pl.col("tissue_name").is_not_null())
            .select([COL_ACCESSION, "tissue_name"])
            .join(tissue_map, on="tissue_name", how="left")
            .filter(pl.col("uberon_id").is_not_null() & (pl.col("uberon_id") != "na"))
        )

        # Load UBERON system descendants for filtering
        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        # Filter to valid UBERON descendants
        before = mapped.height
        mapped_filtered = mapped.filter(pl.col("uberon_id").is_in(valid_uberon))

        self.logger.info(
            "Filtered tissue from %s to %s rows using UBERON system descendants.",
            before,
            mapped_filtered.height,
        )

        # Create tissue annotation records
        tissue_records = mapped_filtered.select(
            pl.col(COL_ACCESSION),
            pl.lit("tissue").alias(COL_ATTRIBUTE),
            pl.col("uberon_id").alias(COL_TERM_ID),
            pl.col("tissue_name").alias(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        self.logger.info("Processed %s tissue annotations", tissue_records.height)
        return tissue_records

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate processed Gu 2023 data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        # Check that expected annotation types are present
        annotation_types = data[COL_ATTRIBUTE].unique().to_list()

        if "disease" not in annotation_types and "tissue" not in annotation_types:
            self.logger.warning(
                "No disease or tissue annotations found in Gu 2023 output."
            )

        # Verify all records have ecode='expert'
        if not all(e == ECODE_EXPERT for e in data[COL_ECODE].unique().to_list()):
            self.logger.warning("Found non-expert ecode values in Gu 2023 data.")

        return True
