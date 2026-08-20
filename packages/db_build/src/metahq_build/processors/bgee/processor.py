"""
Bgee database annotation processor.

Processes RNA-Seq library annotations from the Bgee database (v15.0).
All species are combined in a single annotations file, keyed by
library-level (SRX/ERX/DRX) accession IDs.

Reference: https://bgee.org/
"""

import json
from pathlib import Path
from typing import Any

import polars as pl

from metahq_build.config.config import (
    AGE_KEY,
    BGEE_EXTERNAL_LINKS,
    BGEE_HSAPDV_AGE_GROUP_MAP,
    BGEE_RAW,
    BGEE_SPECIES_IDS,
    BGEE_UBERON_AGE_GROUP_MAP,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    ECODE_EXPERT,
    SEX_FEMALE_ID,
    SEX_KEY,
    SEX_MALE_ID,
    TISSUE_KEY,
    UBERON_OBO,
    UBERON_SYSTEMS,
    VALID_AGE_GROUPS,
    VALID_ORGANISMS,
    VALID_SEXES,
    VALID_TISSUE_ONTOLOGIES,
)
from metahq_build.ontology import get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry

BGEE_DATASET_URL = "https://www.bgee.org/experiment/{}"

COL_BGEE_ID = "#libraryId"
COL_BGEE_EXPERIMENT = "experimentId"
COL_BGEE_SEX = "sex"
COL_BGEE_SPECIES_ID = "speciesId"
COL_BGEE_STAGE_ID = "stageId"
COL_BGEE_STAGE_NAME = "stageName"
COL_BGEE_TISSUE = "anatId"
COL_BGEE_TISSUE_NAME = "anatName"


@ProcessorRegistry.register
class BgeeProcessor(BaseProcessor):
    """
    Processor for Bgee database RNA-Seq library annotations.

    Bgee is a database for gene expression patterns across multiple species,
    providing curated anatomical, developmental stage, and sex annotations
    for RNA-Seq libraries. As of the current Bgee release, all species are
    distributed in a single combined annotations file.
    """

    source_name = "BGee"
    version = "15.2.6"
    description = "Bgee database RNA-Seq library annotations across multiple species"

    def process(self, output_dir: Path, **kwargs: Any) -> pl.DataFrame:
        """Process Bgee RNA-Seq library data into standardized annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                Optional ``bgee_path`` override for the raw annotations file.

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``accession``, ``attribute``, ``term_id``,
                ``term_name``, and ``ecode``.
        """
        self.logger.info("Processing Bgee RNA-Seq library annotations...")

        # Load UBERON/CL system descendants once for tissue filtering
        self.logger.info("Loading UBERON/CL system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        file_path = Path(kwargs.get("bgee_path", BGEE_RAW))
        self.logger.info("Reading Bgee library annotations from %s...", file_path)
        df = pl.read_parquet(file_path).rename({COL_BGEE_ID: COL_ACCESSION})

        # IDs prefixed with '#' are deprecated annotations — drop them
        before = df.height
        df = df.filter(~pl.col(COL_ACCESSION).str.starts_with("#"))
        self.logger.info(
            "Dropped %d deprecated (hash-prefixed) library annotations, kept %d",
            before - df.height,
            df.height,
        )

        # Restrict to species MetaHQ currently supports
        valid_species_ids = self._valid_species_ids()
        before = df.height
        df = df.filter(pl.col(COL_BGEE_SPECIES_ID).is_in(valid_species_ids))
        self.logger.info(
            "Filtered to %d supported organisms: dropped %d rows, kept %d",
            len(valid_species_ids),
            before - df.height,
            df.height,
        )

        urls = self._build_urls(df)

        # Process each annotation type
        tissue_records = self._process_tissue(df, valid_uberon)
        sex_records = self._process_sex(df)
        stage_records = self._process_developmental_stage(df)

        result_df = pl.concat(
            [tissue_records, sex_records, stage_records],
            how="vertical",
        ).sort([COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME])

        self.logger.info("Produced %s total annotations", result_df.height)

        # map ontology terms to uberon
        result_df = self._curate_terms(result_df)

        # save urls
        with open(BGEE_EXTERNAL_LINKS, "w", encoding="utf-8") as f:
            json.dump(urls, f, indent=4, sort_keys=True)

        self.logger.info(
            "Saved external links for %d studies in Bgee to %s",
            len(urls),
            BGEE_EXTERNAL_LINKS,
        )

        # Save processed data
        output_file = output_dir / "bgee_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def _valid_species_ids(self) -> frozenset[int]:
        """Return Bgee ``speciesId`` values for organisms in ``VALID_ORGANISMS``.

        Joins against the Bgee species helper file (shares the ``speciesId``
        column with the raw annotations file) to resolve each ID to a
        ``"{genus} {species}"`` organism name.

        Returns:
            (frozenset[int]): Bgee species IDs for supported organisms.
        """
        species_df = pl.read_csv(BGEE_SPECIES_IDS, separator="\t").with_columns(
            (pl.col("genus") + " " + pl.col("species"))
            .str.to_lowercase()
            .alias("organism")
        )
        species_df = species_df.filter(pl.col("organism").is_in(VALID_ORGANISMS))

        return frozenset(species_df[COL_BGEE_SPECIES_ID].to_list())

    def _build_urls(self, df: pl.DataFrame) -> dict:
        """Build per-study external link records from experiment IDs.

        Arguments:
            df (pl.DataFrame):
                Library-level Bgee annotations with an ``experimentId`` column.

        Returns:
            (dict): Mapping of experiment (study) ID to a Bgee dataset URL record.
        """
        urls: dict = {}
        for experiment_id in df[COL_BGEE_EXPERIMENT].drop_nulls().unique():
            urls.setdefault(experiment_id, {"records": []})
            urls[experiment_id]["records"].append(
                {"id": experiment_id, "url": BGEE_DATASET_URL.format(experiment_id)}
            )

        return urls

    def _curate_terms(self, df: pl.DataFrame) -> pl.DataFrame:
        """Harmonize annotations to UBERON, CL, and age groups."""

        for attribute in [TISSUE_KEY, SEX_KEY, AGE_KEY]:
            unique_ontologies = set(
                df.filter(pl.col(COL_ATTRIBUTE) == attribute)
                .with_columns(pl.col(COL_TERM_ID).str.split(":").list.get(0))[
                    COL_TERM_ID
                ]
                .unique()
                .to_list()
            )
            self.logger.info(
                "Found %d unique ontologies for %s: %s",
                len(unique_ontologies),
                attribute,
                unique_ontologies,
            )

            if attribute == SEX_KEY:
                if not all(term in VALID_SEXES for term in unique_ontologies):
                    raise ValueError(
                        f"Found unexpected sexes in {unique_ontologies}"
                    )  # will create a sex mapper in the future if needed

            if attribute == TISSUE_KEY:
                if not all(
                    term in VALID_TISSUE_ONTOLOGIES for term in unique_ontologies
                ):
                    raise ValueError(
                        f"Found unexpected tissue ontologies in {unique_ontologies}"
                    )  # will create a tissue mapper in the future if needed

            if attribute == AGE_KEY:
                if not all(term in VALID_AGE_GROUPS for term in unique_ontologies):
                    df = self._map_age_terms(df)

        return df

    def _map_age_terms(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map developmental stage term IDs to MetaHQ age groups.

        Only rows with ``attribute == AGE_KEY`` are remapped; tissue and sex
        rows are passed through untouched (a right join against the full
        ``df`` would otherwise drop them, since their term IDs never match
        an age group).
        """
        other_df = df.filter(pl.col(COL_ATTRIBUTE) != AGE_KEY).select(
            [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME, COL_ECODE]
        )
        age_df = df.filter(pl.col(COL_ATTRIBUTE) == AGE_KEY)

        hsapdv_map = (
            pl.scan_csv(BGEE_HSAPDV_AGE_GROUP_MAP)
            .select([COL_TERM_ID, AGE_KEY])
            .filter(pl.col(AGE_KEY) != "na")
            .collect(engine="streaming")
        )
        uberon_map = (
            pl.scan_csv(BGEE_UBERON_AGE_GROUP_MAP)
            .select([COL_TERM_ID, AGE_KEY])
            .filter(pl.col(AGE_KEY) != "na")
            .collect(engine="streaming")
        )

        mappings = pl.concat([hsapdv_map, uberon_map], how="vertical")
        age_df = (
            mappings.join(
                age_df,
                on=COL_TERM_ID,
                how="right",
            )
            .with_columns(pl.coalesce(AGE_KEY, COL_TERM_ID))
            .drop(COL_TERM_ID)
            .rename({AGE_KEY: COL_TERM_ID})
        )

        unmapped = age_df.filter(~pl.col(COL_TERM_ID).is_in(VALID_AGE_GROUPS))[
            COL_TERM_ID
        ].to_list()

        self.logger.warning(
            "Unable to map %d age terms to MetaHQ age groups: %s",
            len(unmapped),
            set(unmapped),
        )

        age_df = (
            age_df.filter(pl.col(COL_TERM_ID).is_in(VALID_AGE_GROUPS))
            .with_columns(pl.col(COL_TERM_ID).alias(COL_TERM_NAME))
            .select(
                [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME, COL_ECODE]
            )
        )

        return pl.concat([other_df, age_df], how="vertical")

    def _process_tissue(
        self, df: pl.DataFrame, valid_uberon: frozenset[str]
    ) -> pl.DataFrame:
        """Process tissue annotations from anatomical entity data.

        Arguments:
            df (pl.DataFrame):
                Library-level Bgee annotations.
            valid_uberon (frozenset[str]):
                Set of valid UBERON/CL term IDs.

        Returns:
            (pl.DataFrame): Tissue annotation records.
        """
        # Filter to rows with valid anatomical entity IDs
        tissue_df = df.filter(
            pl.col(COL_BGEE_TISSUE).is_not_null() & (pl.col(COL_BGEE_TISSUE) != "")
        )

        # Filter to valid UBERON/CL system descendants
        before = tissue_df.height
        tissue_df = tissue_df.filter(pl.col(COL_BGEE_TISSUE).is_in(valid_uberon))

        self.logger.debug(
            "Filtered tissue from %s to %s rows using UBERON/CL system descendants",
            before,
            tissue_df.height,
        )

        # Create tissue annotation records
        tissue_records = tissue_df.select(
            pl.col(COL_ACCESSION),
            pl.lit(TISSUE_KEY).alias(COL_ATTRIBUTE),
            pl.col(COL_BGEE_TISSUE).alias(COL_TERM_ID),
            pl.col(COL_BGEE_TISSUE_NAME).alias(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        return tissue_records

    def _process_sex(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process sex annotations.

        Maps Bgee sex values (``M``, ``F``, ``mixed``, ``NA``) to PATO-aligned
        sex IDs, keeping only unambiguous ``M``/``F`` annotations.

        Arguments:
            df (pl.DataFrame):
                Library-level Bgee annotations.

        Returns:
            (pl.DataFrame): Sex annotation records.
        """
        # Filter to rows with unambiguous sex annotations
        # We'll only process 'M' and 'F', skip 'mixed' and 'NA'
        sex_df = df.filter(pl.col(COL_BGEE_SEX).is_in([SEX_MALE_ID, SEX_FEMALE_ID]))

        # Map to full-word term names
        sex_records = sex_df.with_columns(
            pl.when(pl.col(COL_BGEE_SEX) == SEX_MALE_ID)
            .then(pl.lit("male"))
            .otherwise(pl.lit("female"))
            .alias(COL_TERM_NAME),
        ).select(
            pl.col(COL_ACCESSION),
            pl.lit(SEX_KEY).alias(COL_ATTRIBUTE),
            pl.col(COL_BGEE_SEX).alias(COL_TERM_ID),
            pl.col(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        return sex_records

    def _process_developmental_stage(self, df: pl.DataFrame) -> pl.DataFrame:
        """Process developmental stage annotations.

        Arguments:
            df (pl.DataFrame):
                Library-level Bgee annotations.

        Returns:
            (pl.DataFrame): Developmental stage annotation records.
        """
        # Filter to rows with valid stage annotations
        # Note: These use various species-specific ontologies (HsapDv for
        # human, MmusDv for mouse, UBERON, etc.) — mapping them to MetaHQ
        # age groups is handled downstream in _map_age_terms.
        stage_df = df.filter(
            pl.col(COL_BGEE_STAGE_ID).is_not_null() & (pl.col(COL_BGEE_STAGE_ID) != "")
        )

        # Create developmental stage annotation records
        stage_records = stage_df.select(
            pl.col(COL_ACCESSION),
            pl.lit(AGE_KEY).alias(COL_ATTRIBUTE),
            pl.col(COL_BGEE_STAGE_ID).alias(COL_TERM_ID),
            pl.col(COL_BGEE_STAGE_NAME).alias(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
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
        expected_types = {TISSUE_KEY, SEX_KEY, AGE_KEY}

        for expected_type in expected_types:
            if expected_type not in annotation_types:
                self.logger.warning(
                    "Expected annotation type '%s' not found in output.",
                    expected_type,
                )

        # Verify all records have ecode='expert'
        unique_ecodes = data[COL_ECODE].unique().to_list()
        if unique_ecodes != [ECODE_EXPERT]:
            self.logger.warning(
                "Found non-expert ecode values in Bgee data: %s",
                unique_ecodes,
            )

        # Verify all tissue records
        unique_ontologies = (
            data.filter(pl.col(COL_ATTRIBUTE) == TISSUE_KEY)
            .with_columns(pl.col(COL_TERM_ID).str.split(":").list.get(0))[COL_TERM_ID]
            .unique()
            .to_list()
        )
        if not all(onto in VALID_TISSUE_ONTOLOGIES for onto in unique_ontologies):
            self.logger.warning(
                "Found unsupported ontologies in tissue annotations: %s",
                unique_ontologies,
            )

        # Verify all age records
        unique_ages = (
            data.filter(pl.col(COL_ATTRIBUTE) == AGE_KEY)[COL_TERM_ID]
            .unique()
            .to_list()
        )
        if not all(age_group in VALID_AGE_GROUPS for age_group in unique_ages):
            self.logger.warning(
                "Found unsupported age groups in age annotations: %s",
                unique_ages,
            )

        # Verify all sex records
        unique_sexes = (
            data.filter(pl.col(COL_ATTRIBUTE) == SEX_KEY)[COL_TERM_ID]
            .unique()
            .to_list()
        )
        if not all(sex in VALID_SEXES for sex in unique_sexes):
            self.logger.warning(
                "Found unsupported sex IDs in sex annotations: %s",
                unique_sexes,
            )

        # Check for sample IDs (library-level SRX/ERX/DRX accessions)
        sample_count = data[COL_ACCESSION].n_unique()
        self.logger.info("Validated %s unique samples", sample_count)

        return True
