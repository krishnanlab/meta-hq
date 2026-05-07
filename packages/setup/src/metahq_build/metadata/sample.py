"""
Query sample metadata from OmicIDX.
"""

from pathlib import Path

import polars as pl

from metahq_build.config import (
    DELIMITER,
    OMICIDX_COL_ACCESSION,
    OMICIDX_DB,
    OMICIDX_SAMPLE_TABLE,
)
from metahq_build.metadata.base import BaseMetadataRetriever


class SampleMetadataRetriever(BaseMetadataRetriever):
    """Perform sample-level queries to collect metadata from OmicIDX."""

    def __init__(self, db_path: Path = OMICIDX_DB, table: str = OMICIDX_SAMPLE_TABLE):
        super().__init__(db_path=db_path, table=table)
        self.metadata = pl.DataFrame()

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
        query_standard_fields = [
            field for field in fields if field not in queried_channel_fields
        ]  # remove fields that can be queried outside of channels,
        # but are also in channels (e.g., source_name)

        if len(queried_channel_fields) > 0:
            self.logger.info(
                "Found channel fields to query: %s", queried_channel_fields
            )
            query = self._build_channels_query(
                query_standard_fields,
                accession_name=accession_name,
            )
            self._execute_query(query=query, entries=samples)

            self.logger.info("Unnesting channels metadata...")
            self._unnest_channels(
                standard_fields=query_standard_fields, all_queried_fields=fields
            )
        else:
            query = self._build_base_query(fields, accession_name=accession_name)
            self._execute_query(query=query, entries=samples)

    def _build_channels_query(self, fields: list[str], accession_name: str) -> str:
        sep = ", "
        formatted_fields = sep.join(fields)
        formatted_fields += f"{sep}channels"
        self.logger.info("Querying fields: %s", formatted_fields.split(sep))

        return f"""SELECT {formatted_fields} FROM {self.table} WHERE {accession_name} = ANY($1)"""

    def _unnest_channels(
        self, standard_fields: list[str], all_queried_fields: list[str]
    ):
        """Channel metadata are stored in lists of structs. There is one value per channel.
        Unnest these and any other nested values within (e.g., characteristics)."""

        # extract both channel lists
        self.metadata = self.metadata.with_columns(
            [
                pl.col("channels").list.get(0, null_on_oob=True).alias("ch1"),
                pl.col("channels").list.get(1, null_on_oob=True).alias("ch2"),
            ]
        ).drop("channels")

        new_channel_fields = []
        for channel in ["ch1", "ch2"]:

            # update channel field names to select after unnesting
            _new_channel_fields: dict[str, str] = {
                field: f"{field}_{channel}"
                for field in self.metadata[channel].struct.fields
            }
            new_channel_fields.extend(
                new_field
                for old_field, new_field in _new_channel_fields.items()
                if old_field in all_queried_fields
            )  # only select unnested fields that were originally queried

            # append channel number to field names and unnest
            self.metadata = self.metadata.with_columns(
                pl.col(channel).struct.rename_fields(list(_new_channel_fields.values()))
            ).unnest(channel)

        to_select = standard_fields + new_channel_fields
        self.metadata = self.metadata.select(to_select)

        # unnest and join characteristic structs if applicable
        if "characteristics" in all_queried_fields:
            self._unnest_characteristics(all_queried_fields)

    def _unnest_characteristics(self, all_queried_fields: list[str]):
        """Characteristics for each channel are stored as [{tag: tag1, value: value1}, ...] pairs.
        Join these into 'tag1: value1|tag2: value2|...'
        """
        for characteristics in ["characteristics_ch1", "characteristics_ch2"]:
            if characteristics in all_queried_fields:
                self.metadata = self.metadata.with_columns(
                    pl.col(characteristics)
                    .list.eval(
                        pl.concat_str(
                            pl.element().struct.field("tag"),
                            pl.element().struct.field("value"),
                            separator=": ",
                        )
                    )
                    .list.join(DELIMITER)
                )
