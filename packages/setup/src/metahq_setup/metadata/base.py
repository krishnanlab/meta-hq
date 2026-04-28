"""
Base class for OmicIDX metadata retrieval.
"""

from pathlib import Path

import duckdb
import polars as pl

from metahq_setup.util.logging import setup_logger


class BaseMetadataRetriever:
    """Base class for metadata retrieval from OmicIDX."""

    def __init__(self, db_path: Path, table: str):
        self.db_path = db_path
        self.table = table
        self.metadata = pl.DataFrame()
        self.logger = setup_logger(f"metahq_setup.metadata.{self.__class__.__name__}")

    def get_available_tables(self) -> list[str]:
        """Show available tables in OmicIDX."""
        with duckdb.connect(self.db_path, read_only=True) as conn:
            result = conn.execute(
                "SELECT table_name from information_schema.tables"
            ).fetchall()

        return [i[0] for i in result]

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

    def save(self, file: Path):
        """Save metadata to parquet file."""
        dir_ = file.resolve().parents[0]

        if not dir_.exists():
            dir_.mkdir(exist_ok=True, parents=True)

        self.metadata.write_parquet(file)

    def show_available_fields(self):
        """Print available fields to logger."""
        all_fields, _ = self.get_available_fields()
        self.logger.info("Available fields: %s", all_fields)

    def _build_base_query(self, query_fields: list[str], accession_name: str) -> str:
        formatted_fields = ", ".join(query_fields)
        return f"""SELECT {formatted_fields} FROM {self.table} WHERE {accession_name} = ANY($1)"""

    def _check_fields(self, queried_fields: list[str], available_fields: list[str]):
        ok_fields = []
        for field in queried_fields:
            if field not in available_fields:
                self.logger.warning("%s not in available fields. Skipping...", field)
                continue

            ok_fields.append(field)

        return ok_fields
