"""
Sirota 2011 annotation processor.

Processes expert-curated disease and tissue annotations from Sirota et al. 2011
(doi:10.1126/scitranslmed.3001318). Each row in the source CSV describes a GEO
DataSet (GDS) with comma-separated lists of control and disease GSM IDs. UMLS
disease CUIs are mapped to MONDO and UMLS tissue CUIs are mapped to UBERON via
manually-curated helper files. Both are filtered to system-level ontology
descendants. Control samples receive term_id = MONDO:0000000.
"""

from pathlib import Path
from typing import Any

import polars as pl

from metahq_build.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    ECODE_EXPERT,
    MONDO_OBO,
    MONDO_SYSTEMS,
    SIROTA_2011_CSV,
    SIROTA_UMLS_MONDO,
    SIROTA_UMLS_UBERON,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_build.ontology import get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class Sirota2011Processor(BaseProcessor):
    """
    Processor for Sirota et al. 2011 annotations.

    Explodes comma-separated GSM lists from each GDS row into individual
    sample records, joins with manually-curated UMLS → MONDO and UMLS →
    UBERON mapping files, and filters to system-level ontology descendants.
    """

    source_name = "Sirota_2011"
    version = "1.0.0"
    description = "Sirota 2011 expert-curated disease and tissue annotations"

    def process(self, output_dir: Path, **kwargs: Any) -> pl.DataFrame:
        """
        Process Sirota 2011 CSV into standardized sample annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path | str) — override the default input path
                (defaults to ``SIROTA_2011_CSV`` from config).

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", SIROTA_2011_CSV))
        self.logger.info("Processing Sirota 2011 CSV: %s", input_path)

        df = pl.read_csv(
            input_path,
            columns=["GDS", "DISEASE CUI", "TISSUE CUI", "CONTROL GSM", "DISEASE GSM"],
        )
        df = df.with_columns(("GDS" + pl.col("GDS").cast(pl.String)).alias("GDS"))
        self.logger.info("Read %s rows from Sirota 2011 CSV.", df.height)

        samples = self._explode_samples(df)
        self.logger.info(
            "Exploded to %s unique (sample, disease_cui, tissue_cui) rows.",
            samples.height,
        )

        disease_records = self._build_disease(samples)
        tissue_records = self._build_tissue(samples)

        result_df = pl.concat([disease_records, tissue_records], how="vertical").sort(
            [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME]
        )

        self.logger.info(
            "Produced %s total annotations from Sirota 2011.", result_df.height
        )

        output_file = output_dir / "sirota_2011_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _explode_samples(self, df: pl.DataFrame) -> pl.DataFrame:
        """Explode comma-separated GSM lists into one row per sample."""
        disease = (
            df.with_columns(pl.col("DISEASE GSM").str.split(",").alias("gsm_list"))
            .explode("gsm_list")
            .select(
                ("GSM" + pl.col("gsm_list").str.strip_chars()).alias(COL_ACCESSION),
                pl.col("TISSUE CUI").alias("tissue_cui"),
                pl.col("DISEASE CUI").alias("disease_cui"),
            )
            .filter(pl.col(COL_ACCESSION) != "GSM")
        )

        control = (
            df.with_columns(pl.col("CONTROL GSM").str.split(",").alias("gsm_list"))
            .explode("gsm_list")
            .select(
                ("GSM" + pl.col("gsm_list").str.strip_chars()).alias(COL_ACCESSION),
                pl.col("TISSUE CUI").alias("tissue_cui"),
                pl.lit("control").alias("disease_cui"),
            )
            .filter(pl.col(COL_ACCESSION) != "GSM")
        )

        return pl.concat([disease, control]).unique()

    def _build_disease(self, samples: pl.DataFrame) -> pl.DataFrame:
        """Map UMLS disease CUIs to MONDO and filter to system descendants."""
        self.logger.info("Building disease annotations...")

        mondo_map = pl.read_csv(
            SIROTA_UMLS_MONDO,
            columns=["CUI", "mapped_mondo_id", "mondo_name"],
        )

        disease_samples = samples.filter(pl.col("disease_cui") != "control")
        control_samples = samples.filter(pl.col("disease_cui") == "control")

        disease_df = (
            disease_samples.join(
                mondo_map, left_on="disease_cui", right_on="CUI", how="left"
            )
            .filter(pl.col("mapped_mondo_id").is_not_null())
            .select(
                pl.col(COL_ACCESSION),
                pl.col("mapped_mondo_id").alias(COL_TERM_ID),
                pl.col("mondo_name").alias(COL_TERM_NAME),
            )
        )

        control_df = control_samples.select(
            pl.col(COL_ACCESSION),
            pl.lit("MONDO:0000000").alias(COL_TERM_ID),
            pl.lit("control").alias(COL_TERM_NAME),
        )

        combined = pl.concat([disease_df, control_df]).unique()

        self.logger.info("Loading MONDO system descendants for filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)
        before = combined.height
        combined = combined.filter(
            pl.col(COL_TERM_ID).is_in(valid_mondo)
            | (pl.col("term_id") == "MONDO:0000000")
        )
        self.logger.info(
            "Filtered disease from %s to %s rows using MONDO system descendants.",
            before,
            combined.height,
        )

        records = combined.select(
            pl.col(COL_ACCESSION),
            pl.lit("disease").alias(COL_ATTRIBUTE),
            pl.col(COL_TERM_ID),
            pl.col(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        self.logger.info(
            "Produced %s disease annotations across %s unique samples.",
            records.height,
            records[COL_ACCESSION].n_unique(),
        )
        return records

    def _build_tissue(self, samples: pl.DataFrame) -> pl.DataFrame:
        """Map UMLS tissue CUIs to UBERON and filter to system descendants."""
        self.logger.info("Building tissue annotations...")

        uberon_map = pl.read_csv(
            SIROTA_UMLS_UBERON,
            columns=["TISSUE_CUI", "mapped_uber_id", "uberon_name"],
        )

        tissue_df = (
            samples.join(
                uberon_map, left_on="tissue_cui", right_on="TISSUE_CUI", how="left"
            )
            .filter(pl.col("mapped_uber_id").is_not_null())
            .select(
                pl.col(COL_ACCESSION),
                pl.col("mapped_uber_id").alias(COL_TERM_ID),
                pl.col("uberon_name").alias(COL_TERM_NAME),
            )
            .unique()
        )

        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)
        before = tissue_df.height
        tissue_df = tissue_df.filter(pl.col(COL_TERM_ID).is_in(valid_uberon))
        self.logger.info(
            "Filtered tissue from %s to %s rows using UBERON system descendants.",
            before,
            tissue_df.height,
        )

        records = tissue_df.select(
            pl.col(COL_ACCESSION),
            pl.lit("tissue").alias(COL_ATTRIBUTE),
            pl.col(COL_TERM_ID),
            pl.col(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        self.logger.info(
            "Produced %s tissue annotations across %s unique samples.",
            records.height,
            records[COL_ACCESSION].n_unique(),
        )
        return records

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed Sirota 2011 data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        types = data[COL_ATTRIBUTE].unique().to_list()
        for expected in ["disease", "tissue"]:
            if expected not in types:
                self.logger.warning(
                    "No %s annotations found in Sirota 2011 output.", expected
                )

        return True
