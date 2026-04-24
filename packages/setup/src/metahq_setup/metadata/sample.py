"""
Query sample metadata from OmicIDX.
"""

from pathlib import Path

import duckdb
import polars as pl

from metahq_setup.config.config import OMICIDX_DB
from metahq_setup.metadata.base import BaseMetadataRetriever


class SampleMetadataRetriever(BaseMetadataRetriever):
    """Perform sample-level queries to collect metadata from OmicIDX."""

    def __init__(self, db_path: Path = OMICIDX_DB, table: str = "src_geo_samples"):
        super().__init__()
        self.db_path = db_path
        self.table = table

    def get_available_fields(
        self,
        channel_name: str = "channels",
    ) -> tuple[list[str], list[str]]:
        """Identify the available metadata fields to query.

        Arguments:
            db_path (Path):
                Path to the OmicIDX database.
            table (str):
                Name of the table in OmicIDX to query.
            channel_name (str):
                Name of the channels column.
        Returns:
            (tuple[list[str], list[str]]): All fields available to query and
                channel specific fields are those are hidden in a struct in
                the channel_name column.
        """
        with duckdb.connect(self.db_path, read_only=True) as conn:
            result = conn.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_name = $1;
                """,
                [self.table],
            ).fetchall()

            fields = [row[1] for row in result]
            if channel_name in fields:
                result = conn.execute(
                    f"SELECT unnest({channel_name}) FROM {self.table} LIMIT 1;",
                ).fetchall()

                channel_fields = list(result[0][0].keys())
                fields.extend(channel_fields)
            else:
                channel_fields = []

            return fields, channel_fields

    def get_available_tables(self) -> list[str]:
        """Show available tables in OmicIDX."""
        with duckdb.connect(self.db_path, read_only=True) as conn:
            result = conn.execute(
                "SELECT table_name from information_schema.tables"
            ).fetchall()

        return [i[0] for i in result]

    def retrieve(
        self, fields: list[str], samples: list[str], accession_name: str = "accession"
    ):
        if accession_name not in fields:
            fields.append(accession_name)

        # check all fields are appropriate
        available_fields, channel_fields = self.get_available_fields()

        self.logger.info("Checking queried fields...")
        fields = self._check_fields(fields, available_fields)

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

    def show_available_fields(self):
        all_fields, _ = self.get_available_fields()
        self.logger.info("Available fields: %s", all_fields)

    def _build_base_query(self, query_fields: list[str], accession_name: str) -> str:
        formatted_fields = ", ".join(query_fields)
        return f"""SELECT {formatted_fields} FROM {self.table} WHERE {accession_name} = ANY($1)"""

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

    def _check_fields(self, queried_fields: list[str], available_fields: list[str]):
        ok_fields = []
        for field in queried_fields:
            if field not in available_fields:
                self.logger.warning("%s not in available fields. Skipping...", field)
                continue

            ok_fields.append(field)

        return ok_fields
