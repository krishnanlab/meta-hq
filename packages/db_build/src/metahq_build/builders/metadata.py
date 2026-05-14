"""
Build the metadata files required in the MetaHQ data package.
"""

from pathlib import Path

import bson
import polars as pl

from metahq_build.config import (
    COL_ACCESSION,
    DELIMITER,
    OMICIDX_DB,
    OMICIDX_SAMPLE_TABLE,
    OMICIDX_SERIES_TABLE,
    SAMPLE_ACCESSION_KEY,
    SAMPLE_COMBINED_BSON,
    SAMPLE_METADATA,
    SAMPLE_METADATA_FIELDS,
    SERIES_COMBINED_BSON,
    SERIES_METADATA,
    SERIES_METADATA_FIELDS,
    STUDY_ACCESSION_KEY,
)
from metahq_build.metadata.sample import SampleMetadataRetriever
from metahq_build.metadata.series import SeriesMetadataRetriever
from metahq_build.util.logging import setup_logger


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
        self.sample_metadata = pl.DataFrame()

        self.series_table = series_table
        self.series_metadata = pl.DataFrame()

        self.logger = setup_logger("metahq_build.builders.metadata")

    def build_from_db(
        self,
        sample_db: Path = SAMPLE_COMBINED_BSON,
        series_db: Path = SERIES_COMBINED_BSON,
    ) -> "MetadataBuilder":
        """Build the metadata files."""

        # build sample metadata
        self.logger.info("Building sample metadata...")
        samples = self._load_metahq_db_entries(sample_db)
        self.sample_metadata = (
            self._query_sample(samples, SAMPLE_METADATA_FIELDS)
            .rename({COL_ACCESSION: SAMPLE_ACCESSION_KEY})
            .with_columns(pl.col(pl.String).str.replace_all("\n", " "))
            .sort(SAMPLE_ACCESSION_KEY)
        )

        # build series metadata
        self.logger.info("Building series metadata...")
        series = self._load_metahq_db_entries(series_db)
        self.series_metadata = (
            self._query_series(series, SERIES_METADATA_FIELDS)
            .rename({COL_ACCESSION: STUDY_ACCESSION_KEY})
            .with_columns(
                pl.concat_str(
                    ["Title: " + pl.col("title"), "Summary: " + pl.col("summary")],
                    separator=f" {DELIMITER} ",
                )
                .alias("description")
                .cast(pl.String)
            )  # add description column for backwards compatibility
            .with_columns(pl.col(pl.String).str.replace_all("\n", " "))
        ).sort(STUDY_ACCESSION_KEY)

        return self

    def save(
        self,
        sample_outfile: Path = SAMPLE_METADATA,
        series_outfile: Path = SERIES_METADATA,
    ):
        """Save sample and series metadata for the MetaHQ data package."""
        self.logger.info("Saving metadata...")
        # sample
        if self.sample_metadata.is_empty():
            self.logger.warning("Sample metadata is empty. Run build_from_db() first.")

        self.sample_metadata.write_parquet(sample_outfile)
        self.logger.info("Sample metadata saved to: %s", sample_outfile)

        # series
        if self.series_metadata.is_empty():
            self.logger.warning("Series metadata is empty. Run build_from_db() first.")

        self.series_metadata.write_parquet(series_outfile)
        self.logger.info("Series metadata saved to: %s", series_outfile)

    def _query_sample(
        self, samples: list[str], fields: list[str], null_values: str | None = "NA"
    ):
        """Retrieve sample metadata."""
        retriever = SampleMetadataRetriever(
            db_path=self.db_path, table=self.sample_table
        )
        retriever.retrieve(fields=fields, samples=samples, null_values=null_values)
        return retriever.metadata

    def _query_series(
        self, series: list[str], fields: list[str], null_values: str | None = "NA"
    ):
        """Retrieve series metadata."""
        retriever = SeriesMetadataRetriever(
            db_path=self.db_path, table=self.series_table
        )
        retriever.retrieve(fields=fields, series=series, null_values=null_values)
        return retriever.metadata

    def _load_metahq_db_entries(self, file: Path) -> list[str]:
        """Load samples or series from a MetaHQ BSON database."""
        with open(file, "rb") as f:
            entries = list(bson.decode(f.read()).keys())

        return entries
