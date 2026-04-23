"""
Collapse sample-level annotations to study-level and add study-forward annotations (e.g., Gemma).
"""

from pathlib import Path

import bson
import duckdb

from metahq_setup.combiners.base import BaseAnnotationCombiner
from metahq_setup.combiners.sample import SAMPLE_COMBINED_BSON
from metahq_setup.config.config import (
    OMICIDX_DB,
    PROCESSED_DIR,
    PROCESSED_STUDY_ANNOTATIONS,
)

STUDY_COMBINED_BSON = PROCESSED_DIR / "combined__level-series.bson"


class StudyCombiner(BaseAnnotationCombiner):
    """
    Collapses sample-level annotations to the series-level and adds study-forward annotations.

    Example:
        >>> combiner = StudyCombiner()
        >>> combiner.combine(SAMPLE_COMBINED_BSON).clean().save(STUDY_COMBINED_BSON)
    """

    def combine(
        self, sample_combined_bson: Path = SAMPLE_COMBINED_BSON
    ) -> "StudyCombiner":
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
            for sample in samples:
                pass

        return self

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
