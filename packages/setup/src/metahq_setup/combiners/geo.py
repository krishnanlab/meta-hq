"""
GEO annotation combiner.

Combines processed annotations from all GEO-based sources into a single
BSON file keyed by GSM (sample-level) or GSE (study-level) IDs.
"""

from pathlib import Path

import polars as pl

from metahq_setup.combiners.base import BaseAnnotationCombiner
from metahq_setup.config.config import (
    ALE_PROCESSED,
    CREEDS_PROCESSED,
    DISIGN_ATLAS_PROCESSED,
    GEMMA_PROCESSED,
    GOLIGHTLY_PROCESSED,
    JOHNSON_2023_MICROARRAY_PROCESSED,
    KRISHNANLAB_PROCESSED,
    SIROTA_2011_PROCESSED,
    URSA_PROCESSED,
    URSAHD_PROCESSED,
)

# Maps source name → default processed parquet path.
GEO_SOURCES: dict[str, Path] = {
    "ALE": ALE_PROCESSED,
    "CREEDS": CREEDS_PROCESSED,
    "DiSignAtlas": DISIGN_ATLAS_PROCESSED,
    "Gemma": GEMMA_PROCESSED,
    "Golightly_2018": GOLIGHTLY_PROCESSED,
    "Johnson_2023": JOHNSON_2023_MICROARRAY_PROCESSED,
    "KrishnanLab": KRISHNANLAB_PROCESSED,
    "Sirota_2011": SIROTA_2011_PROCESSED,
    "URSA": URSA_PROCESSED,
    "URSA_HD": URSAHD_PROCESSED,
}


class GeoCombiner(BaseAnnotationCombiner):
    """
    Combines annotations from GEO-based sources.

    All sources in this combiner use GEO accession IDs (GSM or GSE) as
    their primary sample identifier. No ID mapping is required.

    Example:
        >>> combiner = GeoCombiner()
        >>> combiner.combine().clean().save(GEO_COMBINED_BSON)
    """

    def combine(
        self,
        overrides: dict[str, Path] | None = None,
    ) -> "GeoCombiner":
        """
        Load and combine all GEO source parquets.

        Sources whose parquet file does not exist are skipped with a warning.

        Arguments:
            overrides (dict[str, Path] | None):
                Per-source path overrides. Keys are source names from
                ``GEO_SOURCES``; values replace the default path for that source.

        Returns:
            (GeoCombiner): self, for chaining.
        """
        overrides = overrides or {}

        for source_name, default_path in GEO_SOURCES.items():
            path = overrides.get(source_name, default_path)

            if not path.exists():
                self.logger.warning(
                    "Skipping '%s': file not found at %s.", source_name, path
                )
                continue

            self.logger.info("Loading '%s' from %s...", source_name, path)
            data = pl.read_parquet(path)
            self.add_source(source_name, data)

        return self
