"""
Johnson 2023 annotation processor.

Processes manually curated annotations from Johnson et al. 2023
(https://www.biorxiv.org/content/10.1101/2023.01.12.523796v1) for both
microarray (GPL570/GEO) and RNA-seq (refine.bio/SRA) datasets.
"""

from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    JOHNSON_MICROARRAY_MESH_MONDO,
    JOHNSON_MICROARRAY_MESH_UBERON,
    JOHNSON_MICROARRAY_TSV,
    JOHNSON_RNASEQ_DOID_MONDO,
    JOHNSON_RNASEQ_TSV,
    JOHNSON_RNASEQ_UBERON,
    MONDO_OBO,
    MONDO_SYSTEMS,
    PROCESSED_DIR,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_setup.ontology import get_id_map, get_system_descendants
from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class Johnson2023Processor(BaseProcessor):
    """
    Processor for Johnson 2023 manually curated annotations.

    Processes two datasets:
    - Microarray (GPL570): GEO samples with MESH-formatted disease and tissue
    - RNA-seq (refine.bio): SRA samples with DOID disease and free-text tissue

    All annotations have expert curation (ecode='expert').
    """

    source_name = "johnson_2023"
    version = "1.0.0"
    description = "Johnson 2023 manually curated annotations for microarray and RNA-seq"

    def process(self, output_dir: Path = PROCESSED_DIR, **kwargs) -> pl.DataFrame:
        """Process Johnson 2023 datasets into standardized annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet files will be written.
                Defaults to ``data/processed``.
            **kwargs:
                ``microarray_input_path`` (Path) - override microarray input file
                ``rnaseq_input_path`` (Path) - override RNA-seq input file

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        self.logger.info("Processing Johnson 2023 microarray and RNA-seq datasets...")

        # Get input paths (with optional overrides)
        microarray_path = Path(
            kwargs.get("microarray_input_path", JOHNSON_MICROARRAY_TSV)
        )
        rnaseq_path = Path(kwargs.get("rnaseq_input_path", JOHNSON_RNASEQ_TSV))

        # Process microarray data (GPL570/GEO)
        self.logger.info("Processing microarray dataset...")
        microarray_df = self._process_microarray(microarray_path)

        # Process RNA-seq data (refine.bio/SRA)
        self.logger.info("Processing RNA-seq dataset...")
        rnaseq_df = self._process_rnaseq(rnaseq_path)

        self.logger.info(
            "Produced %s microarray and %s RNA-seq annotations.",
            microarray_df.height,
            rnaseq_df.height,
        )

        # Save each dataset separately so geo/sra combiners can consume them
        microarray_file = output_dir / "johnson_2023__microarray.parquet"
        rnaseq_file = output_dir / "johnson_2023__rnaseq.parquet"
        microarray_df.write_parquet(microarray_file)
        self.logger.info("Wrote microarray data to %s", microarray_file)
        rnaseq_df.write_parquet(rnaseq_file)
        self.logger.info("Wrote RNA-seq data to %s", rnaseq_file)

        result_df = pl.concat([microarray_df, rnaseq_df], how="vertical")
        return result_df

    def _process_microarray(self, input_path: Path) -> pl.DataFrame:
        """Process microarray (GPL570/GEO) dataset.

        Arguments:
            input_path (Path):
                Path to microarray TSV file.

        Returns:
            (pl.DataFrame): Standardized annotations for microarray samples.
        """
        # Load microarray data
        df = pl.read_csv(
            input_path,
            separator="\t",
            null_values=["NA", "na", ""],
        )

        # Fill nulls with "na" string for consistent handling
        df = df.fill_null("na")

        self.logger.info(
            "Loaded %s microarray samples from %s",
            df.height,
            input_path.name,
        )

        # Process each annotation type
        disease_records = self._process_microarray_disease(df)
        tissue_records = self._process_microarray_tissue(df)
        sex_records = self._process_sex(df, sample_id_col="gsm")
        age_records = self._process_age(df, sample_id_col="gsm")

        # Combine all annotation types
        all_records = pl.concat(
            [disease_records, tissue_records, sex_records, age_records],
            how="vertical",
        )

        return all_records

    def _process_microarray_disease(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process disease annotations for microarray data.

        Disease column contains pipe-delimited MESH terms like:
        "D000013:Congenital Abnormalities|D009358:Congenital..."

        Arguments:
            df (pl.DataFrame):
                Microarray data with 'gsm' and 'disease' columns.

        Returns:
            (pl.DataFrame): Disease annotation records.
        """
        # Load MESH → MONDO mapping
        mesh_mondo_map = pl.read_csv(JOHNSON_MICROARRAY_MESH_MONDO, separator=",")

        # Explode pipe-delimited disease column
        exploded = (
            df.filter(pl.col("disease") != "na")
            .select(["gsm", "disease"])
            .with_columns(pl.col("disease").str.split("|").alias("disease_list"))
            .explode("disease_list")
            .filter(pl.col("disease_list") != "na")
        )

        # Split "MESH_ID:Name" into separate columns
        exploded = exploded.with_columns(
            pl.col("disease_list").str.split(":").list.get(0).alias("mesh_id"),
            pl.col("disease_list").str.split(":").list.get(1).alias("mesh_name"),
        ).drop("disease_list", "disease")

        # Join with MESH → MONDO mapping
        mapped = exploded.join(mesh_mondo_map, on="mesh_id", how="left")

        # Load MONDO system descendants for filtering
        self.logger.info("Loading MONDO system descendants for disease filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)

        # Filter to valid MONDO descendants
        before = mapped.height
        mapped_filtered = mapped.filter(
            pl.col("mondo_id").is_not_null() & pl.col("mondo_id").is_in(valid_mondo)
        )
        self.logger.info(
            "Filtered microarray disease from %s to %s rows using MONDO system descendants.",
            before,
            mapped_filtered.height,
        )

        # Create disease annotation records - one row per term
        disease_records = mapped_filtered.select(
            pl.col("gsm").alias("sample_id"),
            pl.lit("disease").alias("annotation_type"),
            pl.col("mondo_id").alias("term_id"),
            pl.col("mondo_name").alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info(
            "Processed %s microarray disease annotations", disease_records.height
        )
        return disease_records

    def _process_microarray_tissue(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process tissue annotations for microarray data.

        Tissue column contains pipe-delimited MESH terms like:
        "D014551:Urinary Tract|D014566:Urogenital System"

        Arguments:
            df (pl.DataFrame):
                Microarray data with 'gsm' and 'tissue' columns.

        Returns:
            (pl.DataFrame): Tissue annotation records.
        """
        # Load MESH → UBERON mapping
        mesh_uberon_map = pl.read_csv(JOHNSON_MICROARRAY_MESH_UBERON, separator=",")

        # Explode pipe-delimited tissue column
        exploded = (
            df.filter(pl.col("tissue") != "na")
            .select(["gsm", "tissue"])
            .with_columns(pl.col("tissue").str.split("|").alias("tissue_list"))
            .explode("tissue_list")
            .filter(pl.col("tissue_list") != "na")
        )

        # Split "MESH_ID:Name" into separate columns
        exploded = exploded.with_columns(
            pl.col("tissue_list").str.split(":").list.get(0).alias("mesh_id"),
            pl.col("tissue_list").str.split(":").list.get(1).alias("mesh_name"),
        ).drop("tissue_list", "tissue")

        # Join with MESH → UBERON mapping
        mapped = exploded.join(mesh_uberon_map, on="mesh_id", how="left")

        # Load UBERON system descendants for filtering
        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        # Explode pipe-delimited uber_id to create one row per term
        # Some MESH terms map to multiple UBERON/CL terms (e.g., "UBERON:123|CL:456")
        # Note: uber_name may be null, so we just use "na" as the label
        mapped_exploded = (
            mapped.filter(pl.col("uber_id").is_not_null())
            .with_columns(
                pl.col("uber_id").str.split("|").alias("uber_id_list"),
            )
            .explode("uber_id_list")
            .select(
                [
                    "gsm",
                    pl.col("uber_id_list").alias("uber_id"),
                    pl.lit("na").alias("uber_name"),
                ]
            )
        )

        # Filter to valid UBERON/CL descendants
        before = mapped_exploded.height
        mapped_filtered = mapped_exploded.filter(pl.col("uber_id").is_in(valid_uberon))

        self.logger.info(
            "Filtered microarray tissue from %s to %s rows using UBERON system descendants.",
            before,
            mapped_filtered.height,
        )

        uberon_id_name_map = get_id_map(UBERON_OBO)

        # Create tissue annotation records - one row per term
        tissue_records = mapped_filtered.select(
            pl.col("gsm").alias("sample_id"),
            pl.lit("tissue").alias("annotation_type"),
            pl.col("uber_id").alias("term_id"),
            pl.col("uber_name").alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info(
            "Processed %s microarray tissue annotations", tissue_records.height
        )
        return tissue_records

    def _process_rnaseq(self, input_path: Path) -> pl.DataFrame:
        """Process RNA-seq (refine.bio/SRA) dataset.

        Arguments:
            input_path (Path):
                Path to RNA-seq TSV file.

        Returns:
            (pl.DataFrame): Standardized annotations for RNA-seq samples.
        """
        # Load RNA-seq data
        df = pl.read_csv(
            input_path,
            separator="\t",
            null_values=["NA", "na", ""],
            encoding="latin1",
        )

        # Fill nulls with "na" string for consistent handling
        df = df.fill_null("na")

        self.logger.info(
            "Loaded %s RNA-seq samples from %s",
            df.height,
            input_path.name,
        )

        # Process each annotation type
        disease_records = self._process_rnaseq_disease(df)
        tissue_records = self._process_rnaseq_tissue(df)
        sex_records = self._process_sex(df, sample_id_col="run")
        age_records = self._process_age(df, sample_id_col="run")

        # Combine all annotation types
        all_records = pl.concat(
            [disease_records, tissue_records, sex_records, age_records],
            how="vertical",
        )

        return all_records

    def _process_rnaseq_disease(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process disease annotations for RNA-seq data.

        Disease column contains DOID terms, "normal", "Healthy", or "NA".

        Arguments:
            df (pl.DataFrame):
                RNA-seq data with 'run' and 'disease' columns.

        Returns:
            (pl.DataFrame): Disease annotation records.
        """
        # Load DOID → MONDO mapping (includes normal/Healthy → MONDO:000000)
        doid_mondo_map = pl.read_csv(JOHNSON_RNASEQ_DOID_MONDO, separator=",")

        # Map disease values to MONDO
        mapped = df.select(["run", "disease"]).join(
            doid_mondo_map.select(["original_label", "mondo_id", "mondo_name"]),
            left_on="disease",
            right_on="original_label",
            how="left",
        )

        # Load MONDO system descendants for filtering
        self.logger.info("Loading MONDO system descendants for disease filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)

        # Filter to rows with valid MONDO mappings and system descendants
        # Note: MONDO:000000 (control) should be included even if not in descendants
        before = mapped.filter(pl.col("mondo_id").is_not_null()).height
        disease_records = mapped.filter(
            pl.col("mondo_id").is_not_null()
            & (
                pl.col("mondo_id").is_in(valid_mondo)
                | (pl.col("mondo_id") == "MONDO:000000")
            )
        ).select(
            pl.col("run").alias("sample_id"),
            pl.lit("disease").alias("annotation_type"),
            pl.col("mondo_id").alias("term_id"),
            pl.col("mondo_name").alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info(
            "Filtered RNA-seq disease from %s to %s rows using MONDO system descendants.",
            before,
            disease_records.height,
        )

        return disease_records

    def _process_rnaseq_tissue(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process tissue annotations for RNA-seq data.

        Tissue column contains free-text or UBERON/CL term IDs.

        Arguments:
            df (pl.DataFrame):
                RNA-seq data with 'run' and 'tissue' columns.

        Returns:
            (pl.DataFrame): Tissue annotation records.
        """
        # Load tissue → UBERON/CL mapping
        tissue_uberon_map = pl.read_csv(JOHNSON_RNASEQ_UBERON, separator=",")

        # Map tissue values to UBERON/CL
        mapped = df.select(["run", "tissue"]).join(
            tissue_uberon_map.select(["original_label", "uber_id", "uber_name"]),
            left_on="tissue",
            right_on="original_label",
            how="left",
        )

        # Load UBERON system descendants for filtering
        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        # Explode pipe-delimited uber_id to create one row per term
        # Some tissue labels map to multiple UBERON/CL terms (e.g., "UBERON:123|CL:456")
        # Note: uber_name may be null, so we just use "na" as the label
        mapped_exploded = (
            mapped.filter(pl.col("uber_id").is_not_null())
            .with_columns(
                pl.col("uber_id").str.split("|").alias("uber_id_list"),
            )
            .explode("uber_id_list")
            .select(
                [
                    "run",
                    pl.col("uber_id_list").alias("uber_id"),
                    pl.lit("na").alias("uber_name"),  # Name not important per user
                ]
            )
        )

        # Filter to valid UBERON/CL descendants
        before = mapped_exploded.height
        tissue_records = mapped_exploded.filter(
            pl.col("uber_id").is_in(valid_uberon)
        ).select(
            pl.col("run").alias("sample_id"),
            pl.lit("tissue").alias("annotation_type"),
            pl.col("uber_id").alias("term_id"),
            pl.col("uber_name").alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info(
            "Filtered RNA-seq tissue from %s to %s rows using UBERON system descendants.",
            before,
            tissue_records.height,
        )

        return tissue_records

    def _process_sex(self, df: pl.DataFrame, sample_id_col: str) -> pl.DataFrame:
        """Process sex annotations.

        Normalizes 'male'/'female' to 'M'/'F' and creates sex annotations
        using PATO ontology terms.

        Arguments:
            df (pl.DataFrame):
                Data with sample ID and 'sex' columns.
            sample_id_col (str):
                Name of the sample ID column ('gsm' or 'run').

        Returns:
            (pl.DataFrame): Sex annotation records.
        """
        # Normalize sex values
        sex_df = df.select([sample_id_col, "sex"]).filter(
            pl.col("sex").is_in(["male", "female", "M", "F"])
        )

        sex_df = sex_df.with_columns(
            pl.when(pl.col("sex") == "male")
            .then(pl.lit("M"))
            .when(pl.col("sex") == "female")
            .then(pl.lit("F"))
            .otherwise(pl.col("sex"))
            .alias("normalized_sex")
        )

        # Map to PATO terms (male: PATO:0000384, female: PATO:0000383)
        sex_records = sex_df.with_columns(
            pl.when(pl.col("normalized_sex") == "M")
            .then(pl.lit("PATO:0000384"))
            .when(pl.col("normalized_sex") == "F")
            .then(pl.lit("PATO:0000383"))
            .otherwise(pl.lit(None))
            .alias("term_id"),
            pl.when(pl.col("normalized_sex") == "M")
            .then(pl.lit("male"))
            .when(pl.col("normalized_sex") == "F")
            .then(pl.lit("female"))
            .otherwise(pl.lit(None))
            .alias("term_label"),
        ).select(
            pl.col(sample_id_col).alias("sample_id"),
            pl.lit("sex").alias("annotation_type"),
            pl.col("term_id"),
            pl.col("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info("Processed %s sex annotations", sex_records.height)
        return sex_records

    def _process_age(self, df: pl.DataFrame, sample_id_col: str) -> pl.DataFrame:
        """Process age annotations using age_group bins.

        Uses the age_group column which contains values like:
        fetus, infant, child, adolescent, adult, older_adult, elderly_adult

        Arguments:
            df (pl.DataFrame):
                Data with sample ID and 'age_group' columns.
            sample_id_col (str):
                Name of the sample ID column ('gsm' or 'run').

        Returns:
            (pl.DataFrame): Age annotation records.
        """
        # Filter to samples with valid age_group
        age_df = df.select([sample_id_col, "age_group"]).filter(
            (pl.col("age_group") != "na") & (pl.col("age_group").is_not_null())
        )

        # Create age annotation records using age_group as term_label
        age_records = age_df.select(
            pl.col(sample_id_col).alias("sample_id"),
            pl.lit("age").alias("annotation_type"),
            pl.lit("na").alias("term_id"),  # No standard ontology for age groups
            pl.col("age_group").alias("term_label"),
            pl.lit("expert").alias("ecode"),
        )

        self.logger.info("Processed %s age annotations", age_records.height)
        return age_records

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate that processed Johnson 2023 data meets requirements.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        # Check that all expected annotation types are present
        annotation_types = data["annotation_type"].unique().to_list()
        expected_types = {"disease", "tissue", "sex", "age"}

        for expected_type in expected_types:
            if expected_type not in annotation_types:
                self.logger.warning(
                    "Expected annotation type '%s' not found in output.",
                    expected_type,
                )

        # Verify all records have ecode='expert'
        if not data["ecode"].unique().to_list() == ["expert"]:
            self.logger.warning("Found non-expert ecode values in Johnson 2023 data.")

        return True
