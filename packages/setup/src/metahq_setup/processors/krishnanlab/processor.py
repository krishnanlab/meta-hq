"""
KrishnanLab annotation processor.

Processes expert-curated tissue and disease annotations for GEO samples.
Disease IDs are mapped from DOID to MONDO and filtered to system-level
ontology descendants. Tissue IDs are already in UBERON/CL format and are
filtered to system-level descendants.
"""

from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    KRISHNANLAB_TSV,
    MONDO_OBO,
    MONDO_SYSTEMS,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_setup.ontology import Ontology, get_system_descendants
from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class KrishnanLabProcessor(BaseProcessor):
    """
    Processor for KrishnanLab annotations.

    Provides expert-curated tissue and disease annotations for GEO samples.
    Disease IDs are mapped from DOID to MONDO and filtered to system-level
    descendants. Tissue UBERON/CL IDs are filtered to system-level descendants.
    """

    source_name = "krishnanlab"
    version = "1.0.0"
    description = "KrishnanLab expert-curated tissue and disease annotations"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process KrishnanLab TSV into standardized sample annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path | str) — override the default input path
                (defaults to ``KRISHNANLAB_TSV`` from config).

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", KRISHNANLAB_TSV))
        self.logger.info("Processing KrishnanLab TSV: %s", input_path)

        df = pl.read_csv(input_path, separator="\t", columns=["GSM", "ID", "ID_name", "task"])
        self.logger.info("Read %s rows from KrishnanLab TSV.", df.height)

        tissue_records = self._build_tissue(df)
        disease_records = self._build_disease(df)

        result_df = pl.concat([tissue_records, disease_records], how="vertical")
        self.logger.info(
            "Produced %s total annotations from KrishnanLab.", result_df.height
        )

        output_file = output_dir / "krishnanlab_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_tissue(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter tissue rows to UBERON system descendants."""
        self.logger.info("Building tissue annotations...")

        tissue_df = df.filter(pl.col("task") == "tissue").select(
            pl.col("GSM").alias("sample_id"),
            pl.lit("tissue").alias("annotation_type"),
            pl.col("ID").alias("term_id"),
            pl.col("ID_name").alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)
        before = tissue_df.height
        tissue_df = tissue_df.filter(pl.col("term_id").is_in(valid_uberon))
        self.logger.info(
            "Filtered tissue from %s to %s rows using UBERON system descendants.",
            before,
            tissue_df.height,
        )

        self.logger.info(
            "Produced %s tissue annotations across %s unique samples.",
            tissue_df.height,
            tissue_df["sample_id"].n_unique(),
        )
        return tissue_df

    def _build_disease(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map DOID disease IDs to MONDO and filter to system descendants."""
        self.logger.info("Building disease annotations...")

        disease_df = df.filter(pl.col("task") == "disease")

        self.logger.info("Loading MONDO ontology for DOID -> MONDO mapping...")
        mondo = Ontology.from_obo(MONDO_OBO)
        doid_ids = disease_df["ID"].unique().to_list()
        doid_to_mondo = mondo.map_terms(
            doid_ids, ontology="MONDO", _from="DOID", _to="MONDO"
        )

        unmapped_count = sum(1 for v in doid_to_mondo.values() if v == "NA")
        if unmapped_count:
            self.logger.warning(
                "%s DOID IDs could not be mapped to MONDO and will be dropped.",
                unmapped_count,
            )

        mapped = {k: v for k, v in doid_to_mondo.items() if v != "NA"}
        disease_df = (
            disease_df
            .with_columns(pl.col("ID").replace(mapped).alias("term_id"))
            .filter(pl.col("term_id") != pl.col("ID"))
        )

        self.logger.info("Loading MONDO system descendants for filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)
        before = disease_df.height
        disease_df = disease_df.filter(pl.col("term_id").is_in(valid_mondo))
        self.logger.info(
            "Filtered disease from %s to %s rows using MONDO system descendants.",
            before,
            disease_df.height,
        )

        mondo_names = mondo.class_dict
        records = disease_df.with_columns(
            pl.col("term_id").replace(mondo_names, default="NA").alias("term_label")
        ).select(
            pl.col("GSM").alias("sample_id"),
            pl.lit("disease").alias("annotation_type"),
            pl.col("term_id"),
            pl.col("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info(
            "Produced %s disease annotations across %s unique samples.",
            records.height,
            records["sample_id"].n_unique(),
        )
        return records

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed KrishnanLab data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        types = data["annotation_type"].unique().to_list()
        for expected in ["disease", "tissue"]:
            if expected not in types:
                self.logger.warning(
                    "No %s annotations found in KrishnanLab output.", expected
                )

        return True
