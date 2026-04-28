"""
Query sample metadata from OmicIDX.
"""

from pathlib import Path

import duckdb
import polars as pl

from metahq_setup.config.config import OMICIDX_DB
from metahq_setup.metadata.base import BaseMetadataRetriever


class SeriesMetadataRetriever(BaseMetadataRetriever):
    """Perform series-level queries to collect metadata from OmicIDX."""

    def __init__(self, db_path: Path = OMICIDX_DB, table: str = "src_geo_series"):
        super().__init__(db_path=db_path, table=table)

    def retrieve(
        self, fields: list[str], series: list[str], accession_name: str = "accession"
    ):
        if accession_name not in fields:
            fields.append(accession_name)

        # check all fields are appropriate
        available_fields, _ = self.get_available_fields()

        self.logger.info("Checking queried fields...")
        fields = self._check_fields(fields, available_fields)

        query = self._build_base_query(fields, accession_name=accession_name)

        with duckdb.connect(self.db_path, read_only=True) as conn:
            self.logger.info("Query: %s", query)
            result = conn.execute(query, [series]).fetchall()

        result_dict = {field: [] for field in fields}

        for row in result:
            for i, field in enumerate(fields):
                result_dict[field].append(row[i])

        self.metadata = pl.DataFrame(result_dict)
