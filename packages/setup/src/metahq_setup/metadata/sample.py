"""
Query sample metadata from OmicIDX.
"""

from pathlib import Path

import duckdb
import polars as pl

from metahq_setup.config.config import (
    OMICIDX_COL_ACCESSION,
    OMICIDX_DB,
    OMICIDX_SAMPLE_TABLE,
)
from metahq_setup.metadata.base import BaseMetadataRetriever


class SampleMetadataRetriever(BaseMetadataRetriever):
    """Perform sample-level queries to collect metadata from OmicIDX."""

    def __init__(self, db_path: Path = OMICIDX_DB, table: str = OMICIDX_SAMPLE_TABLE):
        super().__init__(db_path=db_path, table=table)

    def retrieve(
        self,
        fields: list[str],
        samples: list[str],
        accession_name: str = OMICIDX_COL_ACCESSION,
    ):
        """Retrieve sample metadata from OmicIDX."""
        if accession_name not in fields:
            fields.append(accession_name)

        # check all fields are appropriate
        available_fields, channel_fields = self.get_available_fields()

        self.logger.info("Checking queried fields...")
        fields = self._check_fields(fields, available_fields)

        # if necessary, add workaround for channel fields because they are nested
        queried_channel_fields = [field for field in fields if field in channel_fields]
        if len(queried_channel_fields) > 0:
            self.logger.info(
                "Found channel fields to query: %s", queried_channel_fields
            )
            query = self._build_channels_query(
                fields,
                channel_fields=queried_channel_fields,
                accession_name=accession_name,
            )
        else:
            query = self._build_base_query(fields, accession_name=accession_name)

        with duckdb.connect(self.db_path, read_only=True) as conn:
            self.logger.info("Query: %s", query)
            result = conn.execute(query, [samples]).fetchall()

        result_dict = {field: [] for field in fields}

        for row in result:
            for i, field in enumerate(fields):
                result_dict[field].append(row[i])

        self.metadata = pl.DataFrame(result_dict)

    def _build_channels_query(
        self, fields: list[str], channel_fields: list[str], accession_name: str
    ) -> str:

        query_fields = []
        for field in fields:
            if field in channel_fields:
                query_fields.append(f"unnest(channels).{field} AS {field}")
            else:
                query_fields.append(field)

        formatted_fields = ", ".join(query_fields)
        self.logger.info("Querying fields: %s", ", ".join(fields))

        return f"""SELECT {formatted_fields} FROM {self.table} WHERE {accession_name} = ANY($1)"""
