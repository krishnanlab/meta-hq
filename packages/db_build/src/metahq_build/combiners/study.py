"""
Collapse sample-level annotations to study-level and add study-forward annotations (e.g., Gemma).
"""

from pathlib import Path

import bson
import duckdb
import polars as pl

from metahq_build.combiners.base import BaseAnnotationCombiner
from metahq_build.config.config import (
    ACCESSIONS_KEY,
    ALL_METAHQ_KEYS,
    ATTRIBUTE_ANNOTATION_KEYS,
    CONTROL_ID,
    DELIMITER,
    DELTED_STUDIES,
    ECODE_KEY,
    ID_KEY,
    OMICIDX_DB,
    ORGANISM_KEY,
    PLATFORM_ACCESSION_KEY,
    PROCESSED_STUDY_ANNOTATIONS,
    SAMPLE_COMBINED_BSON,
    SRP_ACCESSION_KEY,
    STUDY_ACCESSION_KEY,
)


class StudyCombiner(BaseAnnotationCombiner):
    """
    Collapses sample-level annotations to the series-level and adds study-forward annotations.

    Example:
        >>> combiner = StudyCombiner()
        >>> combiner.combine(SAMPLE_COMBINED_BSON).clean().save(STUDY_COMBINED_BSON)
    """

    def combine(
        self,
        sample_combined_bson: Path = SAMPLE_COMBINED_BSON,
        db_path: Path = OMICIDX_DB,
    ) -> "StudyCombiner":
        """
        Load sample annotations, collapse them, and combine with study-forward annotations.

        Arguments:
            sample_combined_bson (Path):
                Path to the combined sample annotations BSON file generated from the SampleCombiner module.

        Returns:
            (StudyCombiner): self, for chaining.

        """
        self._initialize_study_forward_annotations()
        self._enrich_study_forward_annotations(db_path)

        # add collapsed to new study-forward annotations
        self._add_collapsed_sample_annotations(sample_combined_bson)

        return self

    def _add_collapsed_sample_annotations(self, sample_combined_bson: Path):
        sample_anno = self._load_bson(sample_combined_bson)

        study2sample = self._study2sample_map(sample_anno)
        self.logger.info(
            "Found %d samples from %d studies in %s",
            len(sample_anno),
            len(study2sample),
            sample_combined_bson,
        )

        self.logger.info(
            "%d out of %d studies only have one annotated sample",
            len([k for k, v in study2sample.items() if len(v) == 1]),
            len(study2sample),
        )

        for study, samples in study2sample.items():

            study_anno = (
                {} if study not in self.anno else self.anno[study]
            )  # don't override existing annotations

            for sample in samples:

                if sample not in sample_anno:
                    continue

                for key in ALL_METAHQ_KEYS:

                    # nothing to propagate to study-level
                    if key not in sample_anno[sample]:
                        continue

                    # ==============================================================
                    # ======== Accession IDs
                    # ==============================================================

                    if key == ACCESSIONS_KEY:

                        # doesn't exist so just add
                        if key not in study_anno:
                            study_anno.setdefault(key, {})
                            study_anno[key][STUDY_ACCESSION_KEY] = study
                            study_anno[key][PLATFORM_ACCESSION_KEY] = sample_anno[
                                sample
                            ][key][PLATFORM_ACCESSION_KEY]

                            # not every study has an SRP
                            if SRP_ACCESSION_KEY in sample_anno[sample][key]:
                                study_anno[key][SRP_ACCESSION_KEY] = sample_anno[
                                    sample
                                ][key][SRP_ACCESSION_KEY]

                            continue

                        # append a new platform ID if it already exits
                        if PLATFORM_ACCESSION_KEY in study_anno[key]:
                            existing = set(
                                study_anno[key][PLATFORM_ACCESSION_KEY].split(DELIMITER)
                            )

                            existing.add(
                                sample_anno[sample][ACCESSIONS_KEY][
                                    PLATFORM_ACCESSION_KEY
                                ]
                            )
                            study_anno[key][PLATFORM_ACCESSION_KEY] = DELIMITER.join(
                                existing
                            )
                            continue

                    # ==============================================================
                    # ======== Organism
                    # ==============================================================

                    if key == ORGANISM_KEY:
                        if key not in study_anno:
                            study_anno[key] = sample_anno[sample][key]
                            continue

                        existing = set(study_anno[key].split(DELIMITER))
                        existing.add(sample_anno[sample][key])
                        study_anno[key] = DELIMITER.join(existing)
                        continue

                    # ==============================================================
                    # ======== Attributes (tissue, disease, sex, age, ...)
                    # ==============================================================

                    # attribute annotation doesn't yet exist in the study annotations
                    # so append everything

                    if key not in study_anno:
                        study_anno.setdefault(key, {})

                        for source_name, source_anno in sample_anno[sample][
                            key
                        ].items():

                            # don't add control annotations
                            if source_anno[ID_KEY] == CONTROL_ID:
                                continue

                            study_anno[key][source_name] = source_anno

                    # exists so append to sources
                    elif key in study_anno:
                        for source_name, source_anno in sample_anno[sample][
                            key
                        ].items():
                            # don't add control annotations
                            if source_anno[ID_KEY] == CONTROL_ID:
                                continue

                            # source annotation doesn't exist for the attribute yet
                            # so can just append the entire source annotation
                            if source_name not in study_anno[key]:
                                study_anno[key][source_name] = source_anno
                                continue

                            # source exists so append to existing
                            study_anno[key][source_name] = (
                                self.add_existing_source_annotation(
                                    source_anno, study_anno[key][source_name]
                                )
                            )
            # update the existing annotations if they exist or add new annotations if not
            self.anno[study] = study_anno

    def _initialize_study_forward_annotations(self):
        """Initialize the annotations with study-forward annotations (e.g., Gemma)."""
        for source_name, data in PROCESSED_STUDY_ANNOTATIONS.items():
            data = pl.read_parquet(data)
            self.add_source(source_name, data)

        self._remove_deleted_studies()

    def _remove_deleted_studies(self):
        """Remove any deleted studies from the annotations."""
        self.anno = {
            k: v for k, v in self.anno.items() if k not in self.deleted_studies
        }

    def _enrich_study_forward_annotations(self, db_path: Path):
        self.logger.info("Enriching study-forward accession IDs...")
        if len(self.anno) == 0:
            self.logger.warning(
                "Annotations are empty. No studies to enrich. Skipping..."
            )
        else:
            studies = [
                study for study in self.anno if study not in self.deleted_studies
            ]

            if len(studies) != len(self.anno):
                self.logger.info(
                    "Removed %d studies deleted from GEO.",
                    len(self.anno) - len(studies),
                )
            accession_map = self._build_enrichment_map(studies, db_path)

            enriched = 0
            for row in accession_map.iter_rows(named=True):
                gse = row[ACCESSIONS_KEY]
                srp = row[SRP_ACCESSION_KEY]
                platform = row[PLATFORM_ACCESSION_KEY]
                accession_ids = {
                    STUDY_ACCESSION_KEY: gse,
                    SRP_ACCESSION_KEY: srp,
                    PLATFORM_ACCESSION_KEY: platform,
                }
                accession_ids = {
                    k: v for k, v in accession_ids.items() if v is not None
                }
                organism = row[ORGANISM_KEY]

                if gse in self.anno:
                    self.anno[gse]["accession_ids"].update(accession_ids)
                    self.anno[gse]["organism"] = organism
                    enriched += 1

            self.logger.info(
                "Enriched metadata for %d initial series from sources: %s",
                enriched,
                ", ".join(PROCESSED_STUDY_ANNOTATIONS),
            )

    def _build_enrichment_map(self, gse_ids: list[str], db_path: Path) -> pl.DataFrame:
        """Retrieve SRP/project IDs and organism information from OmicIDX."""
        with duckdb.connect(db_path, read_only=True) as conn:
            accession_map = (
                conn.execute(
                    f"""
                    SELECT
                        accession AS {ACCESSIONS_KEY},
                        sra_studies AS {SRP_ACCESSION_KEY},
                        platform_id AS {PLATFORM_ACCESSION_KEY},
                        sample_organism AS {ORGANISM_KEY}
                    FROM src_geo_series
                    WHERE accession = ANY($1)
                    ORDER BY accession
                """,
                    [gse_ids],
                )
                .pl()
                .lazy()
            )

        # convert empty lists to null join filled lists
        accession_map = (
            accession_map.with_columns(
                pl.when(pl.col(pl.List).list.len() == 0)
                .then(None)
                .otherwise(
                    pl.col(pl.List)
                    .list.join(separator=DELIMITER)
                    .str.strip_chars()
                    .str.strip_chars(
                        '"'
                    )  # for some reason OmicIDX stores SRPs with double quotations
                )
                .name.keep()
            )
            .with_columns(pl.col(ORGANISM_KEY).str.to_lowercase().name.keep())
            .sort(ACCESSIONS_KEY)
        ).collect(engine="streaming")

        self.logger.info(
            "Retrieved accession IDs for %d of %d study-forward annotations from OmicIDX",
            len(accession_map),
            len(gse_ids),
        )
        if not len(accession_map) == len(gse_ids):
            self.logger.warning(
                "Unable to enrich %d studies: %s. Check if these have been deleted or retired from GEO.",
                len(gse_ids) - accession_map.height,
                set(gse_ids) - set(accession_map[ACCESSIONS_KEY].to_list()),
            )

        return accession_map

    def _study2sample_map(self, anno) -> dict[str, list[str]]:
        """Retrieve all studies represented in the combined sample annotations."""
        study2sample: dict[str, list[str]] = {}

        for sample, values in anno.items():
            study_ids = values["accession_ids"]["series"].split("|")
            for study in study_ids:
                study2sample.setdefault(study, [])
                study2sample[study].append(sample)

        return study2sample

    def _load_bson(self, file: Path) -> dict:
        """Load the BSON sample annotation file."""
        if not file.exists():
            raise FileNotFoundError(
                f"Path to combined sample annotations BSON file does not exist: {file}."
            )

        with open(file, "rb") as f:
            data = bson.decode(f.read())

        return data

    @property
    def deleted_studies(self) -> list[str]:
        """Load and return deleted studies. Stored in the DELETED_STUDIES file."""
        result = []
        if len(result) == 0:
            try:
                with open(DELTED_STUDIES, "r", encoding="utf-8") as f:
                    for line in f.readlines():
                        result.append(line.strip())

                return result

            except Exception as e:
                self.logger.error(e)
                return result

        return result

    @staticmethod
    def add_existing_source_annotation(
        source_anno: dict[str, str], study_anno: dict[str, str]
    ) -> dict[str, str]:
        """
        Append an annotation to a source that already exists for a partucular study
        attribute annotation.
        """
        merged_annotations: dict[str, str] = {}
        for key in ATTRIBUTE_ANNOTATION_KEYS:

            if (key not in source_anno) or (source_anno[key] is None):
                continue

            # a source can only have one evidence code
            if key == ECODE_KEY:
                merged_annotations[key] = source_anno[key]

            new: list[str] = source_anno[key].split(DELIMITER)
            if key not in study_anno:
                merged_annotations[key] = DELIMITER.join(new)
                continue

            existing: list[str] = study_anno[key].split(DELIMITER)
            existing.extend(new)
            merged_annotations[key] = DELIMITER.join(set(existing))

        return merged_annotations
