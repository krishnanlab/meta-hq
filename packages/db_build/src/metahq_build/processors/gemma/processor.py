"""
Gemma database annotation processor.

Processes annotations downloaded from the Gemma database
(https://gemma.msl.ubc.ca). Raw annotations must be downloaded first using
``metahq-build download gemma``.
"""

import json
from pathlib import Path
from typing import Any

import polars as pl

from metahq_build.config.config import (
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    DISEASE_KEY,
    ECODE_EXPERT,
    GEMMA_DEV_STAGE_TO_AGE_GROUP,
    GEMMA_EXTERNAL_LINKS,
    GEMMA_RAW,
    GEMMA_SAMPLES_RAW,
    MONDO_OBO,
    MONDO_SYSTEMS,
    PROCESSED_DIR,
    SEX_FEMALE_ID,
    SEX_MALE_ID,
    TISSUE_KEY,
    UBERON_OBO,
    UBERON_SYSTEMS,
    VALID_ONTOLOGIES,
    VALID_SEXES,
)
from metahq_build.ontology import Ontology, get_system_descendants
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

GEMMA_BROWSE_URL = "https://gemma.msl.ubc.ca/browse/#/q/{acc}"
GEMMA_DATASET_URL = "https://gemma.msl.ubc.ca/expressionExperiment/showExpressionExperiment.html?id={acc}"


@ProcessorRegistry.register
class GemmaProcessor(BaseProcessor):
    """
    Processor for Gemma database annotations.

    Raw data must be downloaded before processing using ``metahq-build download gemma``.
    """

    source_name = "Gemma"
    version = "1.32.6"
    description = "Gemma database annotations for gene expression studies"

    def process(self, output_dir: Path = PROCESSED_DIR, **kwargs: Any) -> pl.DataFrame:
        """
        Process Gemma annotations into standardized format.

        Reads from the raw JSON file produced by ``metahq-build download gemma``
        (default location: ``data/unprocessed/gemma.json``). Raises
        ``ProcessorError`` if that file does not exist.

        Writes two parquet files to ``output_dir``: ``gemma_processed.parquet``
        (study-level, accession = GSE) and ``gemma_sample_processed.parquet``
        (sample-level, accession = GSM), using the same column schema. The
        sample-level file only contains annotations attached to individual
        samples or experimental factor values (not present in the
        dataset-level ``characteristics`` list), and is empty if
        ``samples_input_path`` has not been downloaded.

        Arguments:
            output_dir (Path):
                Directory for processed output.
            **kwargs:
                ``input_path`` (Path): override the raw JSON file location.
                ``samples_input_path`` (Path): override the raw per-sample
                characteristics JSON file location produced by
                ``metahq-build download gemma-samples``.

        Returns:
            (pl.DataFrame): Standardized study-level annotations with
                columns ``accession``, ``attribute``, ``term_id``,
                ``term_name``, and ``ecode``.

        Raises:
            (ProcessorError): If the raw Gemma file has not been downloaded.
        """
        input_path = Path(kwargs.get("input_path", GEMMA_RAW))
        samples_input_path = Path(kwargs.get("samples_input_path", GEMMA_SAMPLES_RAW))

        if not input_path.exists():
            raise ProcessorError(
                f"Raw Gemma data not found at {input_path}. "
                "Run 'metahq-build download gemma' first."
            )

        self.logger.info("Processing Gemma annotations from %s...", input_path)

        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        records = []
        urls = {}
        id_to_gse: dict[str, str] = {}
        for batch_data in raw_data.values():
            if not isinstance(batch_data, list):
                continue

            for study in batch_data:
                if not isinstance(study, dict):
                    continue

                gse = study.get("accession", "")
                if not gse:
                    continue

                gemma_id = study.get("id", "")
                if not gemma_id:
                    continue

                id_to_gse[str(gemma_id)] = gse

                # setup urls
                urls.setdefault(gse, {})
                urls[gse].setdefault("records", [])
                if "browse_url" not in urls[gse]:
                    urls[gse]["browse_url"] = GEMMA_BROWSE_URL.format(acc=gse)
                urls[gse]["records"].append(
                    {"id": gemma_id, "url": GEMMA_DATASET_URL.format(acc=gemma_id)}
                )

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

        sample_level = self._load_sample_level_records(samples_input_path, id_to_gse)
        records.extend(
            {
                COL_ACCESSION: r["gse"],
                COL_ATTRIBUTE: r[COL_ATTRIBUTE],
                COL_TERM_ID: r[COL_TERM_ID],
                COL_TERM_NAME: r[COL_TERM_NAME],
                COL_ECODE: r[COL_ECODE],
            }
            for r in sample_level
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
        df = self._curate_terms(df)

        before = len(df)
        df = df.filter(pl.col(COL_ACCESSION).str.starts_with("GSE")).sort(
            [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME]
        )
        self.logger.info(
            "Filtered %d non-GSE annotations (kept %d)",
            before - len(df),
            len(df),
        )
        self.logger.info(
            "There are %d studies represented in Gemma after processing",
            df[COL_ACCESSION].unique().len(),
        )

        # save urls
        urls = {
            study: records
            for study, records in urls.items()
            if study in df[COL_ACCESSION]
        }
        with open(GEMMA_EXTERNAL_LINKS, "w", encoding="utf-8") as f:
            json.dump(urls, f, indent=4, sort_keys=True)

        self.logger.info(
            "Saved external links for %d studies in Gemma to %s",
            len(urls),
            GEMMA_EXTERNAL_LINKS,
        )

        output_file = output_dir / "gemma_processed.parquet"
        df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        # Sample-level annotations (accession = GSM), using the same column
        # schema as the study-level output above. Only annotations attached
        # to individual samples or experimental factor values end up here;
        # see `_load_sample_level_records`.
        sample_df = pl.DataFrame(
            [
                {
                    COL_ACCESSION: r["gsm"],
                    COL_ATTRIBUTE: r[COL_ATTRIBUTE],
                    COL_TERM_ID: r[COL_TERM_ID],
                    COL_TERM_NAME: r[COL_TERM_NAME],
                    COL_ECODE: r[COL_ECODE],
                }
                for r in sample_level
                if r["gse"].startswith("GSE")
            ],
            schema={
                COL_ACCESSION: pl.Utf8,
                COL_ATTRIBUTE: pl.Utf8,
                COL_TERM_ID: pl.Utf8,
                COL_TERM_NAME: pl.Utf8,
                COL_ECODE: pl.Utf8,
            },
        )
        sample_df = self._curate_terms(sample_df)

        # A single sample can't have two sexes. Two-channel Gemma arrays
        # sometimes flatten characteristics from both channels onto one GSM
        # (e.g. a dye-swap comparing a control and a diseased subject),
        # producing conflicting sex values for the same sample. Drop sex for
        # those samples since we can't tell which channel is which.
        conflicting_sex_samples = (
            sample_df.filter(pl.col(COL_ATTRIBUTE) == "sex")
            .group_by(COL_ACCESSION)
            .agg(pl.col(COL_TERM_ID).n_unique().alias("n"))
            .filter(pl.col("n") > 1)[COL_ACCESSION]
        )
        before = len(sample_df)
        sample_df = sample_df.filter(
            ~(
                (pl.col(COL_ATTRIBUTE) == "sex")
                & pl.col(COL_ACCESSION).is_in(conflicting_sex_samples)
            )
        )
        self.logger.info(
            "Dropped sex annotations for %d samples with conflicting sex values (kept %d rows)",
            len(conflicting_sex_samples),
            len(sample_df),
        )

        sample_df = sample_df.sort(
            [COL_ACCESSION, COL_ATTRIBUTE, COL_TERM_ID, COL_TERM_NAME]
        )

        sample_output_file = output_dir / "gemma_sample_processed.parquet"
        sample_df.write_parquet(sample_output_file)
        self.logger.info(
            "Wrote %d sample-level annotations for %d samples to %s",
            len(sample_df),
            sample_df[COL_ACCESSION].n_unique(),
            sample_output_file,
        )

        return df

    def _curate_terms(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply term-level curation shared by study- and sample-level annotations.

        Reclassifies MONDO/DOID-tagged tissue annotations as disease, maps
        developmental-stage terms to MetaHQ age groups, maps PATO sex terms
        to MetaHQ sex IDs, drops a known GSE197511 sex/age term mis-mapping,
        cross-maps tissue/disease terms to UBERON/MONDO, and filters
        tissue/disease annotations to descendants of system-level terms.
        Operates purely on attribute/term_id/term_name and is agnostic to
        whether ``COL_ACCESSION`` holds a study (GSE) or sample (GSM) ID, so
        it is reused for both the study-level and sample-level outputs.

        Arguments:
            df (pl.DataFrame):
                Raw annotation records to curate.

        Returns:
            (pl.DataFrame): Curated annotations.
        """
        df = self._map_mondo_tissue_to_disease(df)
        df = self._map_age_groups(df)
        df = self._map_sex(df)

        # Some Gemma "biological sex" characteristics are miscategorized, pointing
        # to a term from another ontology entirely (e.g. GSE197511 has one mapped
        # to UBERON:0007222 "late adult stage") or to Gemma's own
        # TGEMO:00122 "unspecified factor value" placeholder. Drop sex
        # annotations that didn't map to a valid MetaHQ sex ID.
        before = len(df)
        df = df.filter(
            (pl.col(COL_ATTRIBUTE) != "sex") | pl.col(COL_TERM_ID).is_in(VALID_SEXES)
        )
        self.logger.info(
            "Filtered %d invalid sex annotations (kept %d)", before - len(df), len(df)
        )

        for attribute, onto_obo in {"tissue": UBERON_OBO, "disease": MONDO_OBO}.items():
            terms = (
                df.filter(pl.col(COL_ATTRIBUTE) == attribute)[COL_TERM_ID]
                .unique()
                .to_list()
            )

            ontos = set(sorted({term.split(":")[0] for term in terms}))
            self.logger.info(
                "Found %d unique ontologies represented: %s", len(ontos), ontos
            )
            mapping = self._collect_ontology_mappings(
                Ontology.from_obo(onto_obo), ontos, terms
            )
            if mapping.is_empty():
                self.logger.info(
                    "No cross-ontology mappings found for %s; skipping.", attribute
                )
                continue

            mapping = mapping.with_columns(pl.lit(attribute).alias(COL_ATTRIBUTE))
            counts = mapping.with_columns(
                pl.col(COL_TERM_ID).str.split(":").list.get(0)
            )[COL_TERM_ID].value_counts()

            counts = dict(counts.iter_rows())
            msg = f"Found mappings to ontologies: {counts}"
            self.logger.info(msg)

            df = (
                df.join(
                    mapping,
                    on=[COL_ATTRIBUTE, COL_TERM_ID],
                    how="left",
                )
                .with_columns(pl.coalesce(["mapped", COL_TERM_ID]).alias(COL_TERM_ID))
                .drop("mapped")
                .filter(
                    ~(
                        pl.col(COL_ATTRIBUTE).is_in([TISSUE_KEY, DISEASE_KEY])
                        & ~pl.col(COL_TERM_ID)
                        .str.split(":")
                        .list.get(0)
                        .is_in(list(VALID_ONTOLOGIES))
                    )
                )
            )

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

        return df

    def _load_sample_level_records(
        self, samples_input_path: Path, id_to_gse: dict[str, str]
    ) -> list[dict]:
        """
        Load per-sample characteristics and resolve each to its parent study.

        Reads the raw per-sample characteristics produced by
        ``GemmaFetcher.fetch_samples`` (annotations attached to individual
        samples/bioAssays or experimental factor values, which are not
        present in the dataset-level ``characteristics`` handled above) and
        converts them into the standard annotation columns, keeping both
        the sample's GSM accession and its parent study's GSE accession
        (via ``id_to_gse``) on every record. Callers use ``gse`` to roll
        these up into the study-level output and ``gsm`` to build a
        sample-level output. Missing or unparseable input is logged and
        treated as no additional records, since this file is an
        enhancement on top of the required dataset-level download.

        Arguments:
            samples_input_path (Path):
                Path to the raw per-sample characteristics JSON file.
            id_to_gse (dict[str, str]):
                Mapping of Gemma dataset ID (as string) to GSE accession,
                built while parsing the dataset-level raw file.

        Returns:
            (list[dict]): Records with keys ``gsm``, ``gse``, and the
                standard ``attribute``/``term_id``/``term_name``/``ecode``
                columns.
        """
        if not samples_input_path.exists():
            self.logger.warning(
                "No per-sample Gemma data found at %s; sex/tissue/disease/age "
                "annotations attached only to individual samples or "
                "experimental factor values will be missed. Run "
                "'metahq-build download gemma-samples' to include them.",
                samples_input_path,
            )
            return []

        with open(samples_input_path, "r", encoding="utf-8") as f:
            samples_data = json.load(f)

        records = []
        for dataset_id, characteristics in samples_data.items():
            gse = id_to_gse.get(str(dataset_id))
            if not gse or not isinstance(characteristics, list):
                continue

            for char in characteristics:
                if not isinstance(char, dict):
                    continue

                gsm = char.get("gsm")
                category = char.get("category", "")
                if not gsm or category not in CHARACTERISTICS_MAP:
                    continue

                uri = char.get("valueUri")
                value = char.get("value")
                if not uri or not value:
                    continue

                term_id = uri.split("/")[-1].replace("_", ":")

                records.append(
                    {
                        "gsm": gsm,
                        "gse": gse,
                        COL_ATTRIBUTE: CHARACTERISTICS_MAP[category],
                        COL_TERM_ID: term_id,
                        COL_TERM_NAME: value.lower(),
                        COL_ECODE: ECODE_EXPERT,
                    }
                )

        self.logger.info(
            "Parsed %d sample-level annotations from %s",
            len(records),
            samples_input_path,
        )
        return records

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

    def _map_mondo_tissue_to_disease(self, df: pl.DataFrame) -> pl.DataFrame:
        """Some tissue annotations are to MONDO terms because they originated
        from a diseased tissue (e.g., brain neoplasm). We convert these to disease
        annotations to retain the annotations while respecting the MetaHQ schema and requirements.
        """
        mismatched = df.filter(
            (pl.col(COL_ATTRIBUTE) == "tissue")
            & (
                (pl.col(COL_TERM_ID).str.starts_with("MONDO"))
                | (pl.col(COL_TERM_ID).str.starts_with("DOID"))
            )
        ).height

        self.logger.info(
            "Found %d tissue annotations with MONDO or DOID terms."
            " Converting to disease annotations...",
            mismatched,
        )

        return df.with_columns(
            pl.when(
                (pl.col(COL_ATTRIBUTE) == "tissue")
                & (
                    (pl.col(COL_TERM_ID).str.starts_with("MONDO"))
                    | (pl.col(COL_TERM_ID).str.starts_with("DOID"))
                )
            )
            .then(pl.lit("disease").alias(COL_ATTRIBUTE))
            .otherwise(pl.col(COL_ATTRIBUTE))
        )
