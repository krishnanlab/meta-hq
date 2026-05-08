"""
Gemma database annotation processor.

Processes annotations downloaded from the Gemma database
(https://gemma.msl.ubc.ca). Raw annotations must be downloaded first using
``metahq-build download gemma``.
"""

import json
from pathlib import Path

import polars as pl

from metahq_build.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    ECODE_EXPERT,
    GEMMA_DEV_STAGE_TO_AGE_GROUP,
    GEMMA_RAW,
    MONDO_OBO,
    MONDO_SYSTEMS,
    PROCESSED_DIR,
    SEX_FEMALE_ID,
    SEX_MALE_ID,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_build.ontology import get_system_descendants
from metahq_build.processors.base import BaseProcessor, ProcessorError
from metahq_build.processors.registry import ProcessorRegistry

# Maps Gemma characteristic categories to MetaHQ annotation types.
CHARACTERISTICS_MAP = {
    "disease": "disease",
    "disease model": "disease",
    "cell type": "tissue",
    "developmental stage": "age",
    "organism part": "tissue",
    "biological sex": "sex",
}


PATO_SEX_MAP = {"PATO:0000384": SEX_MALE_ID, "PATO:0000383": SEX_FEMALE_ID}


@ProcessorRegistry.register
class GemmaProcessor(BaseProcessor):
    """
    Processor for Gemma database annotations.

    Gemma is a database of gene expression studies with curated annotations
    for tissues, diseases, developmental stages, sex, and age.

    Raw data must be downloaded before processing:
        metahq-build download gemma
    """

    source_name = "Gemma"
    version = "1.32.6"
    description = "Gemma database annotations for gene expression studies"

    def process(self, output_dir: Path = PROCESSED_DIR, **kwargs) -> pl.DataFrame:
        """
        Process Gemma annotations into standardized format.

        Reads from the raw JSON file produced by ``metahq-build download gemma``
        (default location: ``data/unprocessed/gemma.json``). Raises
        ``ProcessorError`` if that file does not exist.

        Arguments:
            output_dir (Path):
                Directory for processed output.
            **kwargs:
                ``input_path`` (Path): override the raw JSON file location.

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.

        Raises:
            ProcessorError: If the raw Gemma file has not been downloaded.
        """
        input_path = Path(kwargs.get("input_path", GEMMA_RAW))

        if not input_path.exists():
            raise ProcessorError(
                f"Raw Gemma data not found at {input_path}. "
                "Run 'metahq-build download gemma' first."
            )

        self.logger.info("Processing Gemma annotations from %s...", input_path)

        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        records = []
        for batch_data in raw_data.values():
            if not isinstance(batch_data, list):
                continue

            for study in batch_data:
                if not isinstance(study, dict):
                    continue

                gse = study.get("accession", "")
                if not gse:
                    continue

                for char in study.get("characteristics", []):
                    if not isinstance(char, dict):
                        continue

                    category = char.get("category", "")
                    if category not in CHARACTERISTICS_MAP:
                        continue

                    uri = char.get("valueUri")
                    value = char.get("value")
                    if not uri or not value:
                        continue

                    term_id = uri.split("/")[-1].replace("_", ":")

                    records.append(
                        {
                            COL_ACCESSION: gse,
                            COL_ATTRIBUTE: CHARACTERISTICS_MAP[category],
                            COL_TERM_ID: term_id,
                            COL_TERM_NAME: value.lower(),
                            COL_ECODE: ECODE_EXPERT,
                        }
                    )

        df = pl.DataFrame(
            records,
            schema={
                COL_ACCESSION: pl.Utf8,
                COL_ATTRIBUTE: pl.Utf8,
                COL_TERM_ID: pl.Utf8,
                COL_TERM_NAME: pl.Utf8,
                COL_ECODE: pl.Utf8,
            },
        )
        df = self._map_age_groups(df)
        df = self._map_sex(df)

        self.logger.info("Parsed %d annotations from Gemma", len(df))

        # Filter tissue and disease annotations to descendants of system-level terms.
        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        self.logger.info("Loading MONDO system descendants for disease filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)

        before = len(df)
        df = df.filter(
            ~pl.col(COL_ATTRIBUTE).is_in(["tissue", "disease"])
            | (
                (pl.col(COL_ATTRIBUTE) == "tissue")
                & pl.col(COL_TERM_ID).is_in(valid_uberon)
            )
            | (
                (pl.col(COL_ATTRIBUTE) == "disease")
                & pl.col(COL_TERM_ID).is_in(valid_mondo)
            )
        )
        self.logger.info(
            "Filtered %d system-level or above tissue/disease annotations (kept %d)",
            before - len(df),
            len(df),
        )

        before = len(df)
        df = df.filter(pl.col(COL_ACCESSION).str.starts_with("GSE")).sort(COL_ACCESSION)
        self.logger.info(
            "Filtered %d non-GSE annotations (kept %d)",
            before - len(df),
            len(df),
        )

        output_file = output_dir / "gemma_processed.parquet"
        df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed Gemma data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        if len(data) == 0:
            self.logger.warning("No annotations processed from Gemma.")

        return True

    def _map_age_groups(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map term IDs to our pre-defined age groups."""
        age_group_map = (
            pl.read_csv(GEMMA_DEV_STAGE_TO_AGE_GROUP, null_values="na")
            .select([COL_TERM_ID, "age_group"])
            .filter(pl.all_horizontal(pl.col("*").is_not_null()))
        )

        return (
            df.join(age_group_map, on=COL_TERM_ID, how="left")
            .filter(
                (pl.col(COL_ATTRIBUTE) != "age") | pl.col("age_group").is_not_null()
            )
            .with_columns(
                pl.when(pl.col(COL_ATTRIBUTE) == "age")
                .then(pl.col("age_group"))
                .otherwise(pl.col(COL_TERM_ID))
                .alias(COL_TERM_ID)
            )
            .drop("age_group")
            .unique()
            .sort(COL_ACCESSION)
        )

    def _map_sex(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map PATO terms to MetaHQ sex ID constants."""
        return df.with_columns(pl.col(COL_TERM_ID).replace(PATO_SEX_MAP))
