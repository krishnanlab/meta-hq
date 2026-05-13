"""
Query sample metadata from OmicIDX.
"""

from pathlib import Path

import polars as pl

from metahq_build.config import OMICIDX_COL_ACCESSION, OMICIDX_DB, OMICIDX_SERIES_TABLE
from metahq_build.config.config import DELIMITER
from metahq_build.metadata.base import BaseMetadataRetriever


class SeriesMetadataRetriever(BaseMetadataRetriever):
    """Perform series-level queries to collect metadata from OmicIDX."""

    def __init__(self, db_path: Path = OMICIDX_DB, table: str = OMICIDX_SERIES_TABLE):
        super().__init__(db_path=db_path, table=table)
        self.metadata = pl.DataFrame()

    def retrieve(
        self,
        fields: list[str],
        series: list[str],
        accession_name: str = OMICIDX_COL_ACCESSION,
        null_values: str | None = None,
        validate: bool = True,
    ):
        """Retrieve series-level metadata from OmicIDX."""
        if accession_name not in fields:
            fields.append(accession_name)

        # check all fields are appropriate
        available_fields, _ = self.get_available_fields()

        self.logger.info("Checking queried fields...")
        fields = self._check_fields(fields, available_fields)

        self.logger.info("Querying fields: %s", fields)
        query = self._build_base_query(fields, accession_name=accession_name)
        self._execute_query(query=query, entries=series)

        if "sample_id" in self.metadata.columns:
            self.metadata = self.metadata.with_columns(
                pl.col("sample_id").list.join(DELIMITER).alias("sample_id")
            )

        if "platform_id" in self.metadata.columns:
            self.metadata = self.metadata.with_columns(
                pl.col("platform_id").list.join(DELIMITER).alias("platform_id")
            )

        if isinstance(null_values, str):
            self.metadata = self.metadata.fill_null(null_values)

        if validate:
            self.validate()
