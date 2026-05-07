"""
DiSignAtlas annotation processor.

Processes annotations from DiSignAtlas, which provides disease signatures
and tissue annotations for gene expression studies. The source file is a
GMT file where each row encodes a dataset with pipe-delimited metadata in
the second column.
"""

from pathlib import Path

import polars as pl

from metahq_build.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    CONTROL_ID,
    DISIGN_ATLAS_CORRECTIONS,
    DISIGN_ATLAS_GMT,
    DISIGN_ATLAS_TISSUE_MAP,
    ECODE_EXPERT,
    MONDO_OBO,
    MONDO_SYSTEMS,
    PLATFORM_ACCESSION_KEY,
)
from metahq_build.ontology import Ontology, get_system_descendants
from metahq_build.processors.base import BaseProcessor
from metahq_build.processors.registry import ProcessorRegistry

# Pipe-delimited field indices within the description column.
_IDX_GSE = 0
_IDX_GSM_CONTROL = 1
_IDX_GSM_CASE = 2
_IDX_PLATFORM = 3
_IDX_DISEASE_NAME = 5
_IDX_DISEASE_ID = 6
_IDX_TISSUE = 7
_IDX_DATASOURCE = 8
_IDX_LIBRARY_STRATEGY = 9
_IDX_ORGANISM = 10
_EXPECTED_PARTS = 11

_CONTROL_DISEASE_NAME = "Control"
_CONTROL_DISEASE_ID = "C0000000"


def _read_gmt(file_path: Path) -> pl.DataFrame:
    """Read a GMT file and return a DataFrame with dataset_id and description columns.

    GMT files use tab-separated columns where the number of columns per row
    varies, so this function reads line-by-line rather than using a CSV reader.
    Rows with fewer than two tab-separated parts are silently skipped.

    Args:
        file_path (Path):
            Path to the GMT file to read.

    Returns:
        (pl.DataFrame): DataFrame with columns ``dataset_id`` (str) and
            ``description`` (str), one row per valid GMT line.
    """
    dataset_ids: list[str] = []
    descriptions: list[str] = []

    with file_path.open("r") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            dataset_ids.append(parts[0])
            descriptions.append(parts[1])

    return pl.DataFrame(
        {"dataset_id": dataset_ids, "description": descriptions},
        schema={"dataset_id": pl.String, "description": pl.String},
    )


@ProcessorRegistry.register
class DiSignAtlasProcessor(BaseProcessor):
    """
    Processor for DiSignAtlas disease and tissue annotations.

    DiSignAtlas distributes gene-expression disease signatures as a GMT file.
    Each line encodes a dataset with a pipe-delimited metadata string that
    carries GSM sample IDs (split into control and case groups), disease
    terms, tissue, platform, and organism fields.

    The processor expands every dataset into one row per GSM sample and
    produces two annotation types:

    - ``disease`` — for every sample (controls receive a synthetic
      ``Control / C0000000`` term).
    - ``tissue`` — only for samples whose tissue field is present and not
      the literal string ``"None"``.
    """

    source_name = "DiSignAtlas"
    version = "1.0.0"
    description = "DiSignAtlas disease and tissue annotations"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """Process the DiSignAtlas GMT file into standardized sample annotations.

        Reads the GMT file, parses the pipe-delimited description field, expands
        GSM sample IDs for both control and case groups, and emits one disease
        annotation per sample plus one tissue annotation for samples with a valid
        tissue value.

        Args:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path | str) — override the default GMT input path.

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", DISIGN_ATLAS_GMT))
        self.logger.info("Processing DiSignAtlas GMT file: %s", input_path)

        raw_df = _read_gmt(input_path)
        total_rows = raw_df.height
        self.logger.info("Read %s dataset rows from GMT file.", total_rows)

        # Split description on "|" and keep only rows with exactly 11 fields.
        parsed = raw_df.with_columns(
            pl.col("description").str.split("|").alias("parts")
        ).with_columns(pl.col("parts").list.len().alias("parts_len"))

        valid = parsed.filter(pl.col("parts_len") == _EXPECTED_PARTS)
        skipped = total_rows - valid.height
        if skipped > 0:
            self.logger.warning(
                "%s dataset(s) skipped due to unexpected number of description fields "
                "(expected %s pipe-delimited fields).",
                skipped,
                _EXPECTED_PARTS,
            )

        # Extract all named fields from the parts list.
        extracted = valid.with_columns(
            pl.col("parts").list.get(_IDX_GSE).alias("gse"),
            pl.col("parts").list.get(_IDX_GSM_CONTROL).alias("gsm_control"),
            pl.col("parts").list.get(_IDX_GSM_CASE).alias("gsm_case"),
            pl.col("parts").list.get(_IDX_PLATFORM).alias("platform"),
            pl.col("parts").list.get(_IDX_DISEASE_NAME).alias("disease_name"),
            pl.col("parts").list.get(_IDX_DISEASE_ID).alias("disease_id"),
            pl.col("parts").list.get(_IDX_TISSUE).alias("tissue"),
            pl.col("parts").list.get(_IDX_DATASOURCE).alias("datasource"),
            pl.col("parts").list.get(_IDX_LIBRARY_STRATEGY).alias("library_strategy"),
            pl.col("parts").list.get(_IDX_ORGANISM).alias("organism"),
        ).drop("description", "parts", "parts_len")

        # Log datasets that carry the literal string "None" in key fields.
        none_count = extracted.filter(
            (pl.col("tissue") == "None")
            | (pl.col("disease_id") == "None")
            | (pl.col(PLATFORM_ACCESSION_KEY) == "None")
        ).height
        self.logger.info(
            "%s parsed dataset(s) have 'None' in tissue, disease_id, or platform.",
            none_count,
        )

        # Build control rows: explode semicolon-separated GSM IDs and override
        # disease fields with the synthetic control term.
        control_df = (
            extracted.with_columns(
                pl.col("gsm_control").str.split(";").alias(COL_ACCESSION)
            )
            .explode(COL_ACCESSION)
            .filter(pl.col(COL_ACCESSION) != "")
            .with_columns(
                pl.lit(_CONTROL_DISEASE_NAME).alias("disease_name"),
                pl.lit(_CONTROL_DISEASE_ID).alias("disease_id"),
            )
            .drop("gsm_control", "gsm_case")
        )

        # Build case rows: explode semicolon-separated GSM IDs, preserve disease fields.
        case_df = (
            extracted.with_columns(
                pl.col("gsm_case").str.split(";").alias(COL_ACCESSION)
            )
            .explode(COL_ACCESSION)
            .filter(pl.col(COL_ACCESSION) != "")
            .drop("gsm_control", "gsm_case")
        )

        all_samples = pl.concat([control_df, case_df], how="diagonal")

        # Map UMLS disease IDs to MONDO.
        # DiSignAtlas stores bare UMLS IDs (e.g. "C0035335"); add prefix before mapping.
        self.logger.info("Loading MONDO ontology for UMLS -> MONDO mapping...")
        mondo = Ontology.from_obo(MONDO_OBO)
        umls_ids = all_samples["disease_id"].unique().to_list()
        prefixed = ["UMLS:" + uid for uid in umls_ids]
        umls_to_mondo = mondo.map_terms(
            prefixed, ontology="MONDO", _from="UMLS", _to="MONDO"
        )
        # Strip prefix from keys to match the bare IDs stored in the DataFrame.
        bare_to_mondo = {
            uid.removeprefix("UMLS:"): mondo_id
            for uid, mondo_id in umls_to_mondo.items()
        }

        all_samples = all_samples.with_columns(
            pl.col("disease_id").replace(bare_to_mondo).alias("disease_id")
        )

        unmapped = all_samples.filter(pl.col("disease_id") == "NA").height
        if unmapped > 0:
            self.logger.warning(
                "%s sample rows could not be mapped to a MONDO ID.", unmapped
            )

        # Filter to descendants of MONDO system-level terms.
        # Control samples (MONDO:0000000) are retained explicitly.
        self.logger.info("Loading MONDO system descendants for filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)
        before = all_samples.height
        all_samples = all_samples.filter(
            pl.col("disease_id").is_in(valid_mondo)
            | (pl.col("disease_id") == CONTROL_ID)
        )
        self.logger.info(
            "Filtered disease rows from %s to %s using MONDO system descendants.",
            before,
            all_samples.height,
        )

        # Disease annotations — every sample row, including controls.
        disease_records = all_samples.select(
            pl.col(COL_ACCESSION),
            pl.lit("disease").alias(COL_ATTRIBUTE),
            pl.col("disease_id").alias(COL_TERM_ID),
            pl.col("disease_name").str.to_lowercase().alias(COL_TERM_NAME),
            pl.lit(ECODE_EXPERT).alias(COL_ECODE),
        )

        # Load manual tissue name -> term ID mapping and apply via join.
        tissue_name_map = pl.read_csv(DISIGN_ATLAS_TISSUE_MAP)

        # Tissue annotations — only for rows with a non-null, non-"None" tissue.
        tissue_records = (
            all_samples.filter(
                pl.col("tissue").is_not_null() & (pl.col("tissue") != "None")
            )
            .with_columns(pl.col("tissue").str.to_lowercase().alias("tissue"))
            .join(
                tissue_name_map.rename({"name": "tissue", "id": COL_TERM_ID}),
                on="tissue",
                how="left",
            )
            .with_columns(pl.col(COL_TERM_ID).fill_null("na"))
            .select(
                pl.col(COL_ACCESSION),
                pl.lit("tissue").alias(COL_ATTRIBUTE),
                pl.col(COL_TERM_ID),
                pl.col("tissue").alias(COL_TERM_NAME),
                pl.lit(ECODE_EXPERT).alias(COL_ECODE),
            )
        )

        # Apply per-sample corrections, overwriting term_id and term_label where provided.
        corrections = (
            pl.read_csv(DISIGN_ATLAS_CORRECTIONS)
            .rename(
                {
                    "sample": COL_ACCESSION,
                    "term": "corr_term_id",
                    "name": "corr_term_label",
                }
            )
            .unique(subset=[COL_ACCESSION], keep="first")
        )
        tissue_records = (
            tissue_records.join(corrections, on=COL_ACCESSION, how="left")
            .with_columns(
                pl.when(pl.col("corr_term_id").is_not_null())
                .then(pl.col("corr_term_id"))
                .otherwise(pl.col(COL_TERM_ID))
                .alias(COL_TERM_ID),
                pl.when(pl.col("corr_term_label").is_not_null())
                .then(pl.col("corr_term_label"))
                .otherwise(pl.col(COL_TERM_NAME))
                .alias(COL_TERM_NAME),
            )
            .drop("corr_term_id", "corr_term_label")
        )

        result_df = pl.concat([disease_records, tissue_records], how="vertical")

        # only keep GSM sample annotations
        result_df = result_df.filter(pl.col(COL_ACCESSION).str.starts_with("GSM")).sort(
            COL_ACCESSION
        )

        self.logger.info(
            "Produced %s annotations (%s disease, %s tissue) from DiSignAtlas.",
            result_df.height,
            disease_records.height,
            tissue_records.filter(pl.col(COL_TERM_ID) != "na").height,
        )

        output_file = output_dir / "disign_atlas_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate that processed DiSignAtlas data meets minimum requirements.

        Args:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        has_disease = "disease" in data[COL_ATTRIBUTE].unique().to_list()
        if not has_disease:
            self.logger.warning("No disease annotations found in DiSignAtlas output.")

        return True
