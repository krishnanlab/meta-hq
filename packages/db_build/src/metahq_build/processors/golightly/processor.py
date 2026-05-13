"""
Golightly (2018) annotation processor.

Processes clinical sample annotations from the Golightly 2018 dataset, a
collection of GEO studies with expert-curated tissue, sex, and age metadata
stored in per-study clinical text files inside a ZIP archive.
"""

import io
import zipfile
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
    GOLIGHTLY_ZIP,
    PROCESSED_DIR,
    SEX_FEMALE_ID,
    SEX_MALE_ID,
    UBERON_OBO,
    UBERON_SYSTEMS,
)
from metahq_build.ontology import get_system_descendants
from metahq_build.processors.base import BaseProcessor, ProcessorError, ValidationError
from metahq_build.processors.registry import ProcessorRegistry
from metahq_build.util.age_groups import get_age_group

# PATO term IDs and labels for sex annotations.
_SEX_MAP = {
    "F": (SEX_FEMALE_ID, "female"),
    "M": (SEX_MALE_ID, "male"),
}

# Normalizes raw sex strings to canonical M/F keys.
_SEX_NORMALIZE = {
    "male": SEX_MALE_ID,
    "m": SEX_MALE_ID,
    "female": SEX_FEMALE_ID,
    "f": SEX_FEMALE_ID,
}

# Per-file metadata derived from cross-referencing zip file headers with the
# existing processed dataset. Each value is a tuple of:
#   (uberon_id, uberon_name, age_col, sex_col)
# age_col and sex_col are empty string when that file lacks the column.
_FILE_METADATA: dict[str, tuple[str, str, str, str]] = {
    "GSE10320_Clinical.txt": ("UBERON:0002113", "kidney", "", ""),
    "GSE1456_Clinical.txt": ("UBERON:0000310", "breast", "", ""),
    "GSE15296_Clinical.txt": (
        "CL:2000001",
        "peripheral blood mononuclear cell",
        "",
        "",
    ),
    "GSE19804_Clinical.txt": ("UBERON:0002048", "lung", "age", ""),
    "GSE20181_Clinical.txt": ("UBERON:0000310", "breast", "", ""),
    "GSE20189_Clinical.txt": ("UBERON:0002048", "lung", "", ""),
    "GSE2109_Breast_Clinical.txt": (
        "UBERON:0000310",
        "breast",
        "Patient_Age",
        "Gender",
    ),
    "GSE2109_Colon_Clinical.txt": ("UBERON:0001155", "colon", "Patient_Age", "Gender"),
    "GSE2109_Endometrium_Clinical.txt": (
        "UBERON:0001295",
        "endometrium",
        "Patient_Age",
        "",
    ),
    "GSE2109_Kidney_Clinical.txt": (
        "UBERON:0002113",
        "kidney",
        "Patient_Age",
        "Gender",
    ),
    "GSE2109_Lung_Clinical.txt": ("UBERON:0002048", "lung", "Patient_Age", "Gender"),
    "GSE2109_Ovary_Clinical.txt": ("UBERON:0000992", "ovary", "Patient_Age", ""),
    "GSE2109_Prostate_Clinical.txt": (
        "UBERON:0002367",
        "prostate gland",
        "Patient_Age",
        "",
    ),
    "GSE2109_Uterus_Clinical.txt": ("UBERON:0000995", "uterus", "Patient_Age", ""),
    "GSE21510_Clinical.txt": ("UBERON:0012652", "colorectum", "", ""),
    "GSE25507_Clinical.txt": ("CL:0000542", "lymphocyte", "subject_age", ""),
    "GSE26682_U133A_Clinical.txt": ("UBERON:0012652", "colorectum", "age", "gender"),
    "GSE26682_U133PLUS2_Clinical.txt": (
        "UBERON:0012652",
        "colorectum",
        "age",
        "gender",
    ),
    "GSE27279_Clinical.txt": (
        "UBERON:0008788",
        "posterior cranial fossa",
        "age",
        "gender",
    ),
    "GSE27342_Clinical.txt": ("UBERON:0000945", "stomach", "age", "gender"),
    "GSE27854_Clinical.txt": ("UBERON:0012652", "colorectum", "", ""),
    "GSE30219_Clinical.txt": ("UBERON:0002048", "lung", "age_at_surgery", "gender"),
    "GSE30784_Clinical.txt": ("UBERON:0000167", "oral cavity", "age", "Sex"),
    "GSE32646_Clinical.txt": ("UBERON:0000310", "breast", "age", ""),
    "GSE37147_Clinical.txt": ("UBERON:0002185", "bronchus", "age_years", "Sex"),
    "GSE37199_Clinical.txt": ("UBERON:0000178", "blood", "", ""),
    "GSE37892_Clinical.txt": ("UBERON:0001155", "colon", "age_at_diagnosis", "gender"),
    "GSE38958_Clinical.txt": (
        "CL:2000001",
        "peripheral blood mononuclear cell",
        "age",
        "gender",
    ),
    "GSE39491_Clinical.txt": (
        "UBERON:0004921",
        "subdivision of digestive tract",
        "",
        "",
    ),
    "GSE39582_Clinical.txt": (
        "UBERON:0001155",
        "colon",
        "age.at.diagnosis_year",
        "Sex",
    ),
    "GSE40292_Clinical.txt": ("UBERON:0002116", "ileum", "", "gender"),
    "GSE4271_Clinical.txt": ("CL:0000125", "glial cell", "age", "Sex"),
    "GSE43176_Clinical.txt": ("CL:0000738", "leukocyte", "", ""),
    "GSE46449_Clinical.txt": ("CL:0000738", "leukocyte", "age", ""),
    "GSE46691_Clinical.txt": ("UBERON:0002367", "prostate gland", "", ""),
    "GSE46995_Clinical.txt": ("CL:0000738", "leukocyte", "age_of_biopsy", ""),
    "GSE48391_Clinical.txt": ("UBERON:0000310", "breast", "", ""),
    "GSE5460_Clinical.txt": ("UBERON:0000310", "breast", "", ""),
    "GSE5462_Clinical.txt": ("UBERON:0000310", "breast", "", ""),
    "GSE58697_Clinical.txt": (
        "UBERON:0003697",
        "abdominal wall",
        "age_at_diagnosis",
        "Sex",
    ),
    "GSE63885_Clinical.txt": ("UBERON:0000992", "ovary", "", ""),
    "GSE6532_U133A_Clinical.txt": ("UBERON:0000310", "breast", "age", ""),
    "GSE6532_U133PLUS2_Clinical.txt": ("UBERON:0000310", "breast", "age", ""),
    "GSE67784_Clinical.txt": ("UBERON:0000178", "blood", "age", "gender"),
}


@ProcessorRegistry.register
class GolightlyProcessor(BaseProcessor):
    """
    Processor for Golightly (2018) clinical sample annotations.

    Reads a ZIP archive containing per-study clinical text files. Each file
    has a fixed tissue assignment (from ``_FILE_METADATA``) and optionally
    age and sex columns. Age range strings (e.g. ``"40-50"``) are averaged
    before being converted to an age group.
    """

    source_name = "Golightly_2018"
    version = "1.0.0"
    description = "Golightly (2018) expert-curated GEO sample clinical annotations"

    def process(self, output_dir: Path = PROCESSED_DIR, **kwargs: Any) -> pl.DataFrame:
        """
        Process Golightly annotations into standardized format.

        Arguments:
            output_dir (Path):
                Directory for processed output.
            **kwargs:
                ``input_path`` (Path): override the ZIP file location.

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.

        Raises:
            ProcessorError: If the ZIP file does not exist.
        """
        input_path = Path(kwargs.get("input_path", GOLIGHTLY_ZIP))

        if not input_path.exists():
            raise ProcessorError(
                f"Golightly ZIP not found at {input_path}. "
                "Download 'golightly_2018.zip' and place it in data/unprocessed/."
            )

        self.logger.info("Processing Golightly annotations from %s...", input_path)

        self.logger.info("Loading UBERON system descendants for tissue filtering...")
        valid_uberon = get_system_descendants(UBERON_SYSTEMS, UBERON_OBO)

        tissue_frames: list[pl.DataFrame] = []
        sex_frames: list[pl.DataFrame] = []
        age_frames: list[pl.DataFrame] = []

        with zipfile.ZipFile(input_path) as zf:
            for filename in zf.namelist():
                if filename not in _FILE_METADATA:
                    self.logger.debug("Skipping unrecognized file: %s", filename)
                    continue

                uberon_id, uberon_name, age_col, sex_col = _FILE_METADATA[filename]

                with zf.open(filename) as f:
                    raw = f.read()

                df = pl.read_csv(
                    io.BytesIO(raw),
                    separator="\t",
                    null_values=["", "NA", "na", "N/A"],
                    infer_schema_length=10000,
                )

                if "SampleID" not in df.columns:
                    self.logger.warning(
                        "%s: missing 'SampleID' column, skipping.", filename
                    )
                    continue

                df = df.rename({"SampleID": COL_ACCESSION})

                # Tissue: fixed per file, one row per sample.
                if uberon_id in valid_uberon:
                    tissue_frames.append(
                        df.select(COL_ACCESSION).with_columns(
                            pl.lit("tissue").alias(COL_ATTRIBUTE),
                            pl.lit(uberon_id).alias(COL_TERM_ID),
                            pl.lit(uberon_name).alias(COL_TERM_NAME),
                            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
                        )
                    )
                else:
                    self.logger.debug(
                        "%s: term %s (%s) not in UBERON system descendants, skipping tissue.",
                        filename,
                        uberon_id,
                        uberon_name,
                    )

                # Sex annotations.
                if sex_col and sex_col in df.columns:
                    sex_frames.append(
                        self._build_sex(df.select([COL_ACCESSION, sex_col]), sex_col)
                    )

                # Age annotations.
                if age_col and age_col in df.columns:
                    age_frames.append(
                        self._build_age(df.select([COL_ACCESSION, age_col]), age_col)
                    )

        parts = tissue_frames + sex_frames + age_frames
        if not parts:
            self.logger.warning("No annotations produced from Golightly.")
            return pl.DataFrame(
                schema={
                    COL_ACCESSION: pl.Utf8,
                    COL_ATTRIBUTE: pl.Utf8,
                    COL_TERM_ID: pl.Utf8,
                    COL_TERM_NAME: pl.Utf8,
                    COL_ECODE: pl.Utf8,
                }
            )

        result = (
            pl.concat(parts, how="diagonal_relaxed")
            .unique()
            .sort([COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME])
        )

        self.logger.info("Processed %d annotations from Golightly", len(result))

        output_file = output_dir / "golightly_processed.parquet"
        result.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed Golightly data.

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

        if "tissue" not in data[COL_ATTRIBUTE].unique().to_list():
            raise ValidationError("No tissue annotations found in Golightly output.")

        return True

    @staticmethod
    def _build_sex(df: pl.DataFrame, sex_col: str) -> pl.DataFrame:
        """Build sex annotation records from a raw sex column.

        Normalizes ``Male/Female/male/female/M/F`` to PATO terms.

        Arguments:
            df (pl.DataFrame):
                DataFrame with ``sample_id`` and ``sex_col`` columns.
            sex_col (str):
                Name of the sex column.

        Returns:
            (pl.DataFrame): Sex annotation records.
        """
        return (
            df.filter(pl.col(sex_col).is_not_null())
            .with_columns(
                pl.col(sex_col)
                .str.to_lowercase()
                .replace(_SEX_NORMALIZE)
                .alias("_sex_key")
            )
            .filter(pl.col("_sex_key").is_in(list(_SEX_MAP)))
            .select(
                pl.col(COL_ACCESSION),
                pl.lit("sex").alias(COL_ATTRIBUTE),
                pl.col("_sex_key")
                .replace({k: v[0] for k, v in _SEX_MAP.items()})
                .alias(COL_TERM_ID),
                pl.col("_sex_key")
                .replace({k: v[1] for k, v in _SEX_MAP.items()})
                .alias(COL_TERM_NAME),
                pl.lit(ECODE_EXPERT).alias(COL_ECODE),
            )
        )

    @staticmethod
    def _build_age(df: pl.DataFrame, age_col: str) -> pl.DataFrame:
        """Build age annotation records from a raw age column.

        Range strings like ``"40-50"`` are averaged before being passed to
        ``get_age_group``. Plain numeric strings are cast directly.

        Arguments:
            df (pl.DataFrame):
                DataFrame with ``sample_id`` and ``age_col`` columns.
            age_col (str):
                Name of the age column.

        Returns:
            (pl.DataFrame): Age annotation records.
        """
        return (
            df.filter(pl.col(age_col).is_not_null())
            .with_columns(pl.col(age_col).cast(pl.Utf8).alias("_age_str"))
            .with_columns(
                pl.when(pl.col("_age_str").str.contains("-"))
                .then(
                    pl.col("_age_str")
                    .str.split("-")
                    .list.eval(pl.element().cast(pl.Float64, strict=False))
                    .list.mean()
                )
                .otherwise(pl.col("_age_str").cast(pl.Float64, strict=False))
                .alias("_age_years")
            )
            .filter(pl.col("_age_years").is_not_null())
            .with_columns(
                pl.col("_age_years")
                .map_elements(get_age_group, return_dtype=pl.String)
                .alias("age_group")
            )
            .filter(pl.col("age_group").is_not_null())
            .select(
                pl.col(COL_ACCESSION),
                pl.lit("age").alias(COL_ATTRIBUTE),
                pl.col("age_group").alias(COL_TERM_ID),
                pl.col("age_group").alias(COL_TERM_NAME),
                pl.lit(ECODE_EXPERT).alias(COL_ECODE),
            )
        )
