"""
URSA-HD annotation processor.

Processes expert-curated disease, tissue, age, and sex annotations from
URSA-HD. Disease IDs are mapped from MESH to MONDO. Tissue IDs are resolved
via a GSE-level mapping file and, for GSE3526, via raw tissue name extraction
from the sample description. Age and sex are extracted from the free-text
GEO Sample Description using regex.
"""

from pathlib import Path

import polars as pl

from metahq_build.config.config import (
    ATTRIBUTE_KEYS,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    CONTROL_ID,
    CONTROL_VALUE,
    ECODE_EXPERT,
    MONDO_OBO,
    MONDO_SYSTEMS,
    SEX_FEMALE_ID,
    SEX_MALE_ID,
    UBERON_OBO,
    UBERON_SYSTEMS,
    URSAHD_CSV,
    URSAHD_GSE_UBERON,
    URSAHD_RAW_TISSUE,
)
from metahq_build.ontology import Ontology, get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry

_FEMALE_KEYWORDS = [
    "placenta",
    "cervix",
    "trimester",
    "myometrium",
    "hysterectomy",
    "ovarian",
    "ovary",
    "vagina",
    "vulva",
]
_MALE_KEYWORDS = ["prostate"]
_MALE_LABELS = {"male", "m", " m", " male", "m "}
_FEMALE_LABELS = {"female", "f", " f", " female", "f "}


@ProcessorRegistry.register
class URSAHDProcessor(BaseProcessor):
    """
    Processor for URSA-HD annotations.

    URSA-HD provides expert-curated annotations for GEO samples including
    disease (MESH → MONDO), tissue (GSE-level + description-based), age,
    and sex extracted from the GEO Sample Description field.
    """

    source_name = "URSA_HD"
    version = "1.0.0"
    description = "URSA-HD expert-curated disease, tissue, age, and sex annotations"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process the URSA-HD CSV into standardized sample annotations.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path | str) — override the default URSA-HD CSV
                input path (defaults to ``URSAHD_CSV`` from config).

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", URSAHD_CSV))
        self.logger.info("Processing URSA-HD CSV file: %s", input_path)

        df = pl.read_csv(
            input_path,
            columns=["GSMID", "UID", "GSEID", "GEO Sample Description"],
        )
        self.logger.info("Read %s rows from URSA-HD CSV.", df.height)

        base_df = df.select(["GSMID", "GSEID", "GEO Sample Description"])

        disease_records = self._build_disease(df)
        tissue_records = self._build_tissue(base_df)
        age_records = self._build_age(base_df)
        sex_records = self._build_sex(base_df)

        result_df = pl.concat(
            [disease_records, tissue_records, age_records, sex_records],
            how="vertical",
        ).sort([COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME])

        self.logger.info(
            "Produced %s total annotations from URSA-HD.", result_df.height
        )

        output_file = output_dir / "ursahd_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_disease(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map MESH disease IDs to MONDO and filter to system descendants."""
        disease_df = (
            df.with_columns(pl.col("UID").str.split("|").alias("mesh_ids"))
            .explode("mesh_ids")
            .rename({"GSMID": COL_ACCESSION, "mesh_ids": "mesh_id"})
            .drop(["UID", "GSEID", "GEO Sample Description"])
            .filter(pl.col("mesh_id") != "")
        )
        self.logger.info("Loading MONDO ontology for MESH -> MONDO mapping...")
        mondo = Ontology.from_obo(MONDO_OBO)
        mesh_ids = disease_df["mesh_id"].unique().to_list()
        prefixed = ["MESH:" + mid for mid in mesh_ids]
        mesh_to_mondo = mondo.map_terms(
            prefixed, ontology="MONDO", _from="MESH", _to="MONDO"
        )
        bare_to_mondo = {
            mid.removeprefix("MESH:"): (
                "MONDO:0000000" if mondo_id == "control" else mondo_id
            )
            for mid, mondo_id in mesh_to_mondo.items()
        }

        disease_df = disease_df.with_columns(
            pl.col("mesh_id").replace(bare_to_mondo).alias(COL_TERM_ID)
        )

        unmapped = disease_df.filter(pl.col(COL_TERM_ID) == "NA").height
        if unmapped > 0:
            self.logger.warning(
                "%s disease rows could not be mapped to MONDO and will be dropped.",
                unmapped,
            )
        disease_df = disease_df.filter(pl.col(COL_TERM_ID) != "NA")

        self.logger.info("Loading MONDO system descendants for filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)
        before = disease_df.height
        disease_df = disease_df.filter(
            pl.col(COL_TERM_ID).is_in(valid_mondo) | (pl.col("term_id") == CONTROL_ID)
        )
        self.logger.info(
            "Filtered disease from %s to %s rows using MONDO system descendants.",
            before,
            disease_df.height,
        )

        mondo_names = mondo.class_dict
        disease_df = disease_df.with_columns(
            pl.when(pl.col(COL_TERM_ID) == CONTROL_ID)
            .then(pl.lit(CONTROL_VALUE))
            .otherwise(pl.col(COL_TERM_ID).replace(mondo_names, default="NA"))
            .alias(COL_TERM_NAME)
        )

        records = disease_df.select(
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

    def _build_tissue(self, base_df: pl.DataFrame) -> pl.DataFrame:
        """Resolve tissue UBERON IDs via GSE-level mapping and GSE3526 description extraction."""
        self.logger.info("Building tissue annotations...")
        gse_map = pl.read_csv(URSAHD_GSE_UBERON)

        gse_tissue = (
            base_df.with_columns(pl.col("GSEID").str.split("|").alias("gse_list"))
            .explode("gse_list")
            .join(
                gse_map.rename(
                    {
                        "GSE": "gse_list",
                        "UBERON_ID": COL_TERM_ID,
                        "tissue_name": COL_TERM_NAME,
                    }
                ),
                on="gse_list",
                how="left",
            )
            .group_by("GSMID")
            .agg(
                pl.col(COL_TERM_ID).drop_nulls().first(),
                pl.col(COL_TERM_NAME).drop_nulls().first(),
                pl.col("GSEID").first(),
                pl.col("GEO Sample Description").first(),
            )
        )

        raw_map = pl.read_csv(URSAHD_RAW_TISSUE)
        gse3526_tissue = (
            gse_tissue.filter(
                pl.col(COL_TERM_ID).is_null() & pl.col("GSEID").str.contains("GSE3526")
            )
            .with_columns(
                pl.col("GEO Sample Description")
                .str.split("|")
                .list.get(1)
                .str.strip_chars()
                .alias("raw_tissue")
            )
            .join(
                raw_map.rename(
                    {
                        "uberon_id": "term_id_raw",
                        "tissue_uberon": "term_label_raw",
                    }
                ),
                left_on="raw_tissue",
                right_on="raw_tissue",
                how="left",
            )
            .with_columns(
                pl.col("term_id_raw").alias(COL_TERM_ID),
                pl.col("term_label_raw").alias(COL_TERM_NAME),
            )
            .drop(["raw_tissue", "term_id_raw", "term_label_raw"])
        )

        tissue_df = pl.concat(
            [
                gse_tissue.filter(
                    ~(
                        pl.col(COL_TERM_ID).is_null()
                        & pl.col("GSEID").str.contains("GSE3526")
                    )
                ),
                gse3526_tissue,
            ]
        ).filter(pl.col(COL_TERM_ID).is_not_null())

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
            pl.col("GSMID").alias(COL_ACCESSION),
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

    def _build_age(self, base_df: pl.DataFrame) -> pl.DataFrame:
        """Extract age from GEO Sample Description and return standardized records."""
        self.logger.info("Extracting age annotations...")

        desc = pl.col("GEO Sample Description").str.to_lowercase() + "|"

        age_raw = (
            pl.when(desc.str.contains(r"\bage: "))
            .then(desc.str.extract(r"\bage: (.*?)\|"))
            .when(desc.str.contains(r"age \(y\): "))
            .then(desc.str.extract(r"age \(y\): (.*?)\|").str.strip_chars() + " years")
            .when(desc.str.contains(r"block age \(at extraction in years\): "))
            .then(
                desc.str.extract(
                    r"block age \(at extraction in years\): (.*?)\|"
                ).str.strip_chars()
                + " years"
            )
            .when(desc.str.contains(r"age \(months\): "))
            .then(
                desc.str.extract(r"age \(months\): (.*?)\|").str.strip_chars()
                + " months"
            )
            .when(desc.str.contains(r"age_years: "))
            .then(desc.str.extract(r"age_years: (.*?)\|").str.strip_chars() + " years")
            .when(desc.str.contains(r"age\.at\.surgery: "))
            .then(desc.str.extract(r"age\.at\.surgery: (.*?)\|").str.strip_chars())
            .otherwise(None)
        )

        # Normalize to numeric years.
        age_years = (
            pl.when(age_raw.str.contains("months"))
            .then(
                age_raw.str.extract(r"(\d+\.?\d*)").cast(pl.Float64, strict=False) / 12
            )
            .when(age_raw.str.contains("years"))
            .then(age_raw.str.extract(r"(\d+\.?\d*)").cast(pl.Float64, strict=False))
            .when(age_raw.str.contains(r"\d"))
            .then(age_raw.str.extract(r"(\d+\.?\d*)").cast(pl.Float64, strict=False))
            .otherwise(None)
        )

        # Bin numeric age into age group labels.
        age_group = (
            pl.when(pl.col("age_years").is_between(-1, 0))
            .then(pl.lit("fetus"))
            .when(pl.col("age_years").is_between(0, 2))
            .then(pl.lit("infant"))
            .when(pl.col("age_years").is_between(2, 10))
            .then(pl.lit("child"))
            .when(pl.col("age_years").is_between(10, 20))
            .then(pl.lit("adolescent"))
            .when(pl.col("age_years").is_between(20, 50))
            .then(pl.lit("adult"))
            .when(pl.col("age_years").is_between(50, 80))
            .then(pl.lit("older_adult"))
            .when(pl.col("age_years").is_between(80, 150))
            .then(pl.lit("elderly_adult"))
            .otherwise(None)
        )

        age_df = (
            base_df.with_columns(age_years.alias("age_years"))
            .filter(pl.col("age_years").is_not_null())
            .with_columns(age_group.alias("age_group"))
            .filter(pl.col("age_group").is_not_null())
            .select(
                pl.col("GSMID").alias(COL_ACCESSION),
                pl.lit("age").alias(COL_ATTRIBUTE),
                pl.col("age_group").alias(COL_TERM_ID),
                pl.col("age_group").alias(COL_TERM_NAME),
                pl.lit(ECODE_EXPERT).alias(COL_ECODE),
            )
        )

        self.logger.info(
            "Produced %s age annotations across %s unique samples.",
            age_df.height,
            age_df[COL_ACCESSION].n_unique(),
        )
        return age_df

    def _build_sex(self, base_df: pl.DataFrame) -> pl.DataFrame:
        """Extract sex from GEO Sample Description and return standardized records."""
        self.logger.info("Extracting sex annotations...")

        desc = pl.col("GEO Sample Description").str.to_lowercase() + "|"

        # Regex-based extraction.
        sex_raw = (
            pl.when(desc.str.contains(r"sex:"))
            .then(desc.str.extract(r"sex:\s*(.*?)\|"))
            .when(desc.str.contains(r"sex="))
            .then(desc.str.extract(r"sex=\s*(.*?)\|"))
            .when(desc.str.contains(r"gender:\s*(female|male|f\b|m\b)"))
            .then(desc.str.extract(r"gender:\s*(female|male|f\b|m\b)"))
            .otherwise(None)
        )

        # Keyword fallbacks where regex found nothing.
        has_female = pl.any_horizontal(
            [desc.str.contains(r"\|female\|")]
            + [desc.str.contains(kw) for kw in _FEMALE_KEYWORDS]
        )
        has_male = pl.any_horizontal(
            [desc.str.contains(r"\|male\|")]
            + [desc.str.contains(kw) for kw in _MALE_KEYWORDS]
        )

        sex_with_fallback = (
            pl.when(sex_raw.is_not_null())
            .then(sex_raw)
            .when(has_female)
            .then(pl.lit("female"))
            .when(has_male)
            .then(pl.lit("male"))
            .otherwise(None)
        )

        # Normalize
        sex_normalized = (
            pl.when(sex_with_fallback.str.strip_chars().is_in(_MALE_LABELS))
            .then(pl.lit(SEX_MALE_ID))
            .when(sex_with_fallback.str.strip_chars().is_in(_FEMALE_LABELS))
            .then(pl.lit(SEX_FEMALE_ID))
            .otherwise(None)
        )

        sex_df = (
            base_df.with_columns(sex_normalized.alias("sex"))
            .filter(pl.col("sex").is_not_null())
            .select(
                pl.col("GSMID").alias(COL_ACCESSION),
                pl.lit("sex").alias(COL_ATTRIBUTE),
                pl.lit("na").alias(COL_TERM_ID),
                pl.col("sex").alias(COL_TERM_NAME),
                pl.lit(ECODE_EXPERT).alias(COL_ECODE),
            )
        )

        sex_df = sex_df.with_columns(
            pl.when(pl.col(COL_TERM_NAME) == SEX_FEMALE_ID)
            .then(pl.lit(SEX_FEMALE_ID))
            .otherwise(
                pl.when(pl.col(COL_TERM_NAME) == SEX_MALE_ID)
                .then(pl.lit(SEX_MALE_ID))
                .otherwise(pl.lit(None))
            )
            .alias(COL_TERM_ID),
        )

        self.logger.info(
            "Produced %s sex annotations across %s unique samples.",
            sex_df.height,
            sex_df[COL_ACCESSION].n_unique(),
        )
        return sex_df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed URSA-HD data.

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
        for expected in ["disease", "tissue", "age", "sex"]:
            if expected not in types:
                self.logger.warning(
                    "No %s annotations found in URSA-HD output.", expected
                )

        return True
