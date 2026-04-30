"""
Build the metadata files required in the MetaHQ data package.
"""

from pathlib import Path

import bson

from metahq_setup.config import (
    OMICIDX_DB,
    OMICIDX_SAMPLE_TABLE,
    OMICIDX_SERIES_TABLE,
    SAMPLE_COMBINED_BSON,
    SAMPLE_METADATA,
    SERIES_COMBINED_BSON,
    SERIES_METADATA,
)
from metahq_setup.metadata.sample import SampleMetadataRetriever
from metahq_setup.metadata.series import SeriesMetadataRetriever

SAMPLE_METADATA_FIELDS: list[str] = [
    "accession",
    "title",
    "platform_id",
    "description",
    "source_name",
    "characteristics",
]
SERIES_METADATA_FIELDS: list[str] = [
    "accession",
    "title",
    "summary",
    "overall_design",
    "sample_id",
    "platform_id",
]


class MetadataBuilder:
    """Build the metadata files required in the MetaHQ data package.


    Attributes:
        db_path (Path):
            Path to OmicIDX DuckDB database.

        sample_table (str):
            Name of the samples metadata table in OmicIDX.

        series_table (str):
            Name of the series metadata table in OmicIDX.

    """

    def __init__(
        self,
        db_path: Path = OMICIDX_DB,
        sample_table: str = OMICIDX_SAMPLE_TABLE,
        series_table: str = OMICIDX_SERIES_TABLE,
    ):
        self.db_path = db_path
        self.sample_table = sample_table
        self.series_table = series_table

    def build_from_db(
        self,
        sample_db: Path = SAMPLE_COMBINED_BSON,
        series_db: Path = SERIES_COMBINED_BSON,
    ):
        """Build the metadata files."""

        # build sample metadata
        samples = self._load_metahq_db_entries(sample_db)
        self._query_sample(samples)

        # build series metadata
        series = self._load_metahq_db_entries(series_db)

    def save(
        self,
        sample_outfile: Path = SAMPLE_METADATA,
        series_outfile: Path = SERIES_METADATA,
    ):
        """Save sample and series metadata for the MetaHQ data package."""

    def _query_sample(
        self,
        samples: list[str],
        fields: list[str] = SAMPLE_METADATA_FIELDS,
    ):
        retriever = SampleMetadataRetriever(
            db_path=self.db_path, table=self.sample_table
        )
        retriever.retrieve(fields=fields, samples=samples)
        pass

    def _query_series(self):
        pass

    def _load_metahq_db_entries(self, file: Path) -> list[str]:
        """Load samples or series from a MetaHQ BSON database."""
        with open(file, "rb") as f:
            entries = list(bson.decode(f.read()).keys())

        return entries
