"""
Bgee database annotation processor.

Processes RNA-Seq library annotations from the Bgee database (v15.0)
for multiple species including mouse, human, rat, worm, and fish.

Reference: https://bgee.org/
"""

from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    BGEE_FISH,
    BGEE_FLY,
    BGEE_HUMAN,
    BGEE_MOUSE,
    BGEE_RAT,
    BGEE_WORM,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_setup.ontology import get_system_descendants
from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class BgeeProcessor(BaseProcessor):
    """
    Processor for Bgee database RNA-Seq library annotations.

    Bgee is a database for gene expression patterns across multiple species,
    providing curated anatomical, developmental stage, and sex annotations
    for RNA-Seq libraries.

    Processes data for 6 species:
    - Mus musculus (mouse)
    - Homo sapiens (human)
    - Rattus norvegicus (rat)
    - Caenorhabditis elegans (worm)
    - Danio rerio (zebrafish)
    - Drosophila melanogaster (fly)
    """

    source_name = "bgee"
    version = "1.0.0"
    description = "Bgee database RNA-Seq library annotations across multiple species"

    # Species configuration
    SPECIES_FILES = {
        "mouse": BGEE_MOUSE,
        "human": BGEE_HUMAN,
        "rat": BGEE_RAT,
        "worm": BGEE_WORM,
        "fish": BGEE_FISH,
        "fly": BGEE_FLY,
    }

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """Process Bgee RNA-Seq library data into standardized annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                Optional species-specific file path overrides.

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        self.logger.info("Processing Bgee RNA-Seq library annotations...")

        # Load UBERON system descendants once for all species
        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        # Process each species
        all_species_data = []
        for species_name, default_path in self.SPECIES_FILES.items():
            # Allow override via kwargs
            file_path = Path(kwargs.get(f"{species_name}_path", default_path))

            if not file_path.exists():
                self.logger.warning(
                    "Skipping %s: file not found at %s",
                    species_name,
                    file_path,
                )
                continue

            self.logger.info(
                "Processing %s data from %s...", species_name, file_path.name
            )
            species_df = self._process_species(file_path, valid_uberon, species_name)
            all_species_data.append(species_df)

            self.logger.info(
                "Processed %s annotations for %s",
                species_df.height,
                species_name,
            )

        # Combine all species
        if not all_species_data:
            self.logger.error("No species data was successfully processed!")
            return pl.DataFrame(
                schema={
                    COL_ACCESSION: pl.Utf8,
                    COL_ATTRIBUTE: pl.Utf8,
                    COL_TERM_ID: pl.Utf8,
                    COL_TERM_NAME: pl.Utf8,
                    COL_ECODE: pl.Utf8,
                }
            )

        result_df = pl.concat(all_species_data, how="vertical").sort(COL_ACCESSION)

        self.logger.info(
            "Produced %s total annotations across %s species",
            result_df.height,
            len(all_species_data),
        )

        # Save processed data
        output_file = output_dir / "bgee_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def _process_species(
        self, file_path: Path, valid_uberon: frozenset[str]
    ) -> pl.DataFrame:
        """Process a single species RNA-Seq library file.

        Arguments:
            file_path (Path):
                Path to the species TSV file.
            valid_uberon (frozenset[str]):
                Set of valid UBERON/CL term IDs from system descendants.
            species_name (str):
                Name of the species (for metadata/logging).

        Returns:
            (pl.DataFrame): Standardized annotations for this species.
        """
        # Read TSV file
        df = pl.read_csv(
            file_path,
            separator="\t",
            null_values=["NA", "na", ""],
        )

        # Select and rename columns we need
        df = df.select(
            [
                "Run IDs",
                "Expression mapped anatomical entity ID",
                "Expression mapped anatomical entity name",
                "Expression mapped stage ID",
                "Expression mapped stage name",
                "Expression mapped sex",
            ]
        )

        # Explode pipe-delimited Run IDs column
        # First split the Run IDs into a list
        df = df.with_columns(pl.col("Run IDs").str.split("|").alias("run_id_list"))

        # Explode the list to create one row per run ID
        df_exploded = df.explode("run_id_list")

        # Rename for clarity
        df_exploded = df_exploded.rename({"run_id_list": COL_ACCESSION})

        # Drop the original "Run IDs" column
        df_exploded = df_exploded.drop("Run IDs")

        # Filter out any null sample IDs
        df_exploded = df_exploded.filter(pl.col(COL_ACCESSION).is_not_null())

        # Process each annotation type
        tissue_records = self._process_tissue(df_exploded, valid_uberon)
        sex_records = self._process_sex(df_exploded)
        stage_records = self._process_developmental_stage(df_exploded)

        # Combine all annotation types
        all_records = pl.concat(
            [tissue_records, sex_records, stage_records],
            how="vertical",
        )

        return all_records

    def _process_tissue(
        self, df: pl.DataFrame, valid_uberon: frozenset[str]
    ) -> pl.DataFrame:
        """Process tissue annotations from anatomical entity data.

        Arguments:
            df (pl.DataFrame):
                Exploded data with sample IDs and expression mapped columns.
            valid_uberon (frozenset[str]):
                Set of valid UBERON/CL term IDs.

        Returns:
            (pl.DataFrame): Tissue annotation records.
        """
        # Filter to rows with valid anatomical entity IDs
        tissue_df = df.filter(
            pl.col("Expression mapped anatomical entity ID").is_not_null()
            & (pl.col("Expression mapped anatomical entity ID") != "")
        )

        # Filter to valid UBERON/CL system descendants
        before = tissue_df.height
        tissue_df = tissue_df.filter(
            pl.col("Expression mapped anatomical entity ID").is_in(valid_uberon)
        )

        self.logger.debug(
            "Filtered tissue from %s to %s rows using UBERON system descendants",
            before,
            tissue_df.height,
        )

        # Create tissue annotation records
        tissue_records = tissue_df.select(
            pl.col(COL_ACCESSION),
            pl.lit("tissue").alias(COL_ATTRIBUTE),
            pl.col("Expression mapped anatomical entity ID").alias(COL_TERM_ID),
            pl.col("Expression mapped anatomical entity name").alias(COL_TERM_NAME),
            pl.lit("expert").alias(COL_ECODE),
        )

        return tissue_records

    def _process_sex(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process sex annotations.

        Maps Bgee sex values (male, female, hermaphrodite, mixed, not annotated)
        to PATO ontology terms.

        Arguments:
            df (pl.DataFrame):
                Exploded data with sample IDs and expression mapped sex.

        Returns:
            (pl.DataFrame): Sex annotation records.
        """
        # Filter to rows with valid sex annotations
        # We'll only process 'male' and 'female', skip 'mixed', 'hermaphrodite', 'not annotated'
        sex_df = df.filter(pl.col("Expression mapped sex").is_in(["male", "female"]))

        # Map to PATO terms
        sex_records = sex_df.with_columns(
            pl.when(pl.col("Expression mapped sex") == "male")
            .then(pl.lit("PATO:0000384"))
            .when(pl.col("Expression mapped sex") == "female")
            .then(pl.lit("PATO:0000383"))
            .otherwise(pl.lit(None))
            .alias(COL_TERM_ID),
            pl.when(pl.col("Expression mapped sex") == "male")
            .then(pl.lit("male"))
            .when(pl.col("Expression mapped sex") == "female")
            .then(pl.lit("female"))
            .otherwise(pl.lit(None))
            .alias(COL_TERM_NAME),
        ).select(
            pl.col(COL_ACCESSION),
            pl.lit("sex").alias(COL_ATTRIBUTE),
            pl.col(COL_TERM_ID),
            pl.col(COL_TERM_NAME),
            pl.lit("expert").alias(COL_ECODE),
        )

        return sex_records

    def _process_developmental_stage(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process developmental stage annotations.

        Arguments:
            df (pl.DataFrame):
                Exploded data with sample IDs and expression mapped stage.

        Returns:
            (pl.DataFrame): Developmental stage annotation records.
        """
        # Filter to rows with valid stage annotations
        stage_df = df.filter(
            pl.col("Expression mapped stage ID").is_not_null()
            & (pl.col("Expression mapped stage ID") != "")
        )

        # Create developmental stage annotation records
        # Note: These use various ontologies (MmusDv for mouse, HsapDv for human, UBERON, etc.)
        stage_records = stage_df.select(
            pl.col(COL_ACCESSION),
            pl.lit("developmental_stage").alias(COL_ATTRIBUTE),
            pl.col("Expression mapped stage ID").alias(COL_TERM_ID),
            pl.col("Expression mapped stage name").alias(COL_TERM_NAME),
            pl.lit("expert").alias(COL_ECODE),
        )

        return stage_records

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate that processed Bgee data meets requirements.

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
        expected_types = {"tissue", "sex", "developmental_stage"}

        for expected_type in expected_types:
            if expected_type not in annotation_types:
                self.logger.warning(
                    "Expected annotation type '%s' not found in output.",
                    expected_type,
                )

        # Verify all records have ecode='expert'
        unique_ecodes = data[COL_ECODE].unique().to_list()
        if unique_ecodes != ["expert"]:
            self.logger.warning(
                "Found non-expert ecode values in Bgee data: %s",
                unique_ecodes,
            )

        # Check for sample IDs (should all be SRR format)
        sample_count = data[COL_ACCESSION].n_unique()
        self.logger.info("Validated %s unique samples", sample_count)

        return True
