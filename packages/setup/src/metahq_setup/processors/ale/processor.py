"""
ALE (Giles et al.) annotation processor.

Processes expert-curated sample annotations from the ALE study, including
tissue (mapped from BTO to UBERON/CL), sex, and age annotations.
"""

from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    ALE_BTO_UBERON,
    ALE_TSV,
    PROCESSED_DIR,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_setup.ontology import get_system_descendants
from metahq_setup.processors.base import BaseProcessor, ValidationError
from metahq_setup.processors.registry import ProcessorRegistry
from metahq_setup.util.age_groups import get_age_group

# PATO term IDs and labels for sex annotations.
_SEX_MAP = {
    "F": ("PATO:0000383", "female"),
    "M": ("PATO:0000384", "male"),
}


@ProcessorRegistry.register
class ALEProcessor(BaseProcessor):
    """
    Processor for ALE (Giles et al.) manual annotations.

    ALE provides expert-curated annotations for GEO samples. Tissue terms
    are stored as BTO IDs and mapped to UBERON/CL using a helper file.
    Age in months is converted to years and then to a discrete age group.
    """

    source_name = "ale"
    version = "1.0.0"
    description = "ALE (Giles et al.) manually curated GEO sample annotations"

    def process(self, output_dir: Path = PROCESSED_DIR, **kwargs) -> pl.DataFrame:
        """
        Process ALE annotations into standardized format.

        Reads the raw ALE TSV, maps BTO tissue IDs to UBERON/CL via the
        helper CSV, maps sex codes to PATO terms, and converts age in months
        to age group labels.

        Arguments:
            output_dir (Path):
                Directory for processed output.
            **kwargs:
                ``input_path`` (Path): override the raw TSV location.
                ``bto_uberon_path`` (Path): override the BTO→UBERON map location.

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", ALE_TSV))
        bto_uberon_path = Path(kwargs.get("bto_uberon_path", ALE_BTO_UBERON))

        self.logger.info("Processing ALE annotations from %s...", input_path)

        # Read raw TSV (first row is a header with no value for the GSM column).
        df = pl.read_csv(
            input_path,
            separator="\t",
            skip_rows=1,
            new_columns=["sample_id", "brenda", "age_months", "gender"],
            schema_overrides={"age_months": pl.Float64},
            null_values=["", "na", "NA"],
            infer_schema_length=40000,
        ).unique()

        # Map BTO → UBERON/CL (inner join drops samples with unmapped BTO terms).
        bto_map = pl.read_csv(bto_uberon_path).select(
            ["brenda", "uberon", "uberon_name"]
        )
        df = df.join(bto_map, on="brenda", how="inner")

        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        tissue_df = self._build_tissue(df, valid_uberon)
        sex_df = self._build_sex(df)
        age_df = self._build_age(df)

        result = pl.concat([tissue_df, sex_df, age_df], how="vertical")

        self.logger.info("Processed %d annotations from ALE", len(result))

        output_file = output_dir / "ale_processed.parquet"
        result.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed ALE data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing or tissue
                annotations are absent.
        """
        self._validate_required_columns(data)

        if "tissue" not in data["annotation_type"].unique().to_list():
            raise ValidationError("No tissue annotations found in ALE output.")

        return True

    def _build_tissue(
        self, df: pl.DataFrame, valid_uberon: frozenset[str]
    ) -> pl.DataFrame:
        """Build tissue annotation records from the joined BTO→UBERON columns.

        Filters to only UBERON/CL terms that are descendants of system-level
        terms, dropping annotations at the system level or higher.

        Arguments:
            df (pl.DataFrame):
                Joined ALE + BTO→UBERON DataFrame.
            valid_uberon (frozenset[str]):
                Descendant term IDs from UBERON system-level terms.

        Returns:
            (pl.DataFrame): Tissue annotation records.
        """
        before = df.filter(pl.col("uberon").is_not_null()).height
        tissue_df = (
            df.filter(
                pl.col("uberon").is_not_null()
                & pl.col("uberon").is_in(valid_uberon)
            )
            .select(
                pl.col("sample_id"),
                pl.lit("tissue").alias("annotation_type"),
                pl.col("uberon").alias("term_id"),
                pl.col("uberon_name").alias("term_label"),
                pl.lit("expert").alias("ecode"),
            )
        )
        self.logger.info(
            "Filtered %d system-level or above tissue annotations (kept %d)",
            before - tissue_df.height,
            tissue_df.height,
        )
        return tissue_df

    @staticmethod
    def _build_sex(df: pl.DataFrame) -> pl.DataFrame:
        """Build sex annotation records, mapping F/M to PATO terms."""
        return df.filter(pl.col("gender").is_in(list(_SEX_MAP))).select(
            pl.col("sample_id"),
            pl.lit("sex").alias("annotation_type"),
            pl.col("gender")
            .replace({k: v[0] for k, v in _SEX_MAP.items()})
            .alias("term_id"),
            pl.col("gender")
            .replace({k: v[1] for k, v in _SEX_MAP.items()})
            .alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

    @staticmethod
    def _build_age(df: pl.DataFrame) -> pl.DataFrame:
        """Build age annotation records, converting months → years → age group."""
        return (
            df.filter(pl.col("age_months").is_not_null())
            .with_columns(
                (pl.col("age_months") / 12)
                .map_elements(get_age_group, return_dtype=pl.String)
                .alias("age_group")
            )
            .filter(pl.col("age_group").is_not_null())
            .select(
                pl.col("sample_id"),
                pl.lit("age").alias("annotation_type"),
                pl.col("age_group").alias("term_id"),
                pl.col("age_group").alias("term_label"),
                pl.lit("expert").alias("ecode"),
            )
        )
