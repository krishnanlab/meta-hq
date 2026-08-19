"""
GEO annotation combiner.

Combines processed annotations from all GEO-based sources into a single
BSON file keyed by GSM (sample-level) or GSE (study-level) IDs.
"""

from pathlib import Path

import polars as pl

from metahq_build.combiners.base import BaseAnnotationCombiner
from metahq_build.config.config import (
    ALE_PROCESSED,
    CREEDS_PROCESSED,
    DISIGN_ATLAS_PROCESSED,
    GEMMA_PROCESSED,
    GEMMA_SAMPLE_PROCESSED,
    GOLIGHTLY_PROCESSED,
    JOHNSON_2023_MICROARRAY_PROCESSED,
    KRISHNANLAB_PROCESSED,
    SIROTA_2011_PROCESSED,
    URSA_PROCESSED,
    URSAHD_PROCESSED,
)

# Maps source name → default sample-level (GSM) processed parquet path.
GEO_SOURCES: dict[str, Path] = {
    "ALE": ALE_PROCESSED,
    "CREEDS": CREEDS_PROCESSED,
    "DiSignAtlas": DISIGN_ATLAS_PROCESSED,
    "Gemma": GEMMA_SAMPLE_PROCESSED,
    "Golightly_2018": GOLIGHTLY_PROCESSED,
    "Johnson_2023": JOHNSON_2023_MICROARRAY_PROCESSED,
    "KrishnanLab": KRISHNANLAB_PROCESSED,
    "Sirota_2011": SIROTA_2011_PROCESSED,
    "URSA": URSA_PROCESSED,
    "URSA_HD": URSAHD_PROCESSED,
}

# Maps source name → default study-level (GSE) processed parquet path, for
# sources that annotate directly at the study level (currently only Gemma).
GEO_STUDY_SOURCES: dict[str, Path] = {
    "Gemma": GEMMA_PROCESSED,
}


class GeoCombiner(BaseAnnotationCombiner):
    """
    Combines annotations from GEO-based sources.

    All sources in this combiner use GEO accession IDs (GSM or GSE) as
    their primary sample identifier. No ID mapping is required.

    Example
        >>> combiner = GeoCombiner()
        >>> combiner.combine().clean().save(GEO_COMBINED_BSON)
    """

    def combine(
        self,
        overrides: dict[str, Path] | None = None,
        study_overrides: dict[str, Path] | None = None,
    ) -> "GeoCombiner":
        """
        Load and combine all GEO source parquets.

        Loads sample-level (GSM) sources from ``GEO_SOURCES`` and
        study-level (GSE) sources from ``GEO_STUDY_SOURCES``. Sources whose
        parquet file does not exist are skipped with a warning.

        Arguments:
            overrides (dict[str, Path] | None):
                Per-source path overrides for ``GEO_SOURCES``. Keys are
                source names; values replace the default path for that
                source.
            study_overrides (dict[str, Path] | None):
                Per-source path overrides for ``GEO_STUDY_SOURCES``.

        Returns:
            (GeoCombiner): self, for chaining.
        """
        self._load_sources(GEO_SOURCES, overrides or {})
        self._load_sources(GEO_STUDY_SOURCES, study_overrides or {})

        return self

    def _load_sources(
        self, sources: dict[str, Path], overrides: dict[str, Path]
    ) -> None:
        """Load and add each source's parquet, skipping missing files with a warning."""
        for source_name, default_path in sources.items():
            path = overrides.get(source_name, default_path)

            if not path.exists():
                self.logger.warning(
                    "Skipping '%s': file not found at %s.", source_name, path
                )
                continue

            self.logger.info("Loading '%s' from %s...", source_name, path)
            data = pl.read_parquet(path)
            self.add_source(source_name, data)
