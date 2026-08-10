"""
Abstract base class for Curation export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2026-06-23 by Parker Hicks
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import polars as pl

from metahq_core.util.io import load_bson
from metahq_core.util.supported import (
    database_ids,
    geo_metadata,
    geo_metadata_fields,
    get_annotations,
    metadata_fields,
    supported,
)

if TYPE_CHECKING:
    from metahq_core.curations.base import BaseCuration
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


class BaseExporter(ABC):
    """Base abstract class for Exporter children.

    Concrete subclasses are expected to set ``self.log`` (a `logging.Logger`)
    and ``self.verbose`` (bool) before any of the concrete helpers below are
    used.
    """

    @abstractmethod
    def to_json(
        self,
        curation: BaseCuration,
        file: FilePath,
        metadata: str | None,
        *args,
        **kwargs,
    ):
        """Saves curation as json."""

    @abstractmethod
    def to_numpy(self, curation: BaseCuration) -> NpIntMatrix:
        """Returns curations matrix as numpy array."""

    @abstractmethod
    def to_parquet(
        self, curation: BaseCuration, file: FilePath, metadata: str | None, **kwargs
    ):
        """Saves curation to parquet."""

    @abstractmethod
    def to_csv(
        self,
        curation: BaseCuration,
        file: FilePath,
        metadata: str | None,
        **kwargs,
    ):
        """Saves curation to csv."""

    @abstractmethod
    def to_tsv(
        self,
        curation: BaseCuration,
        file: FilePath,
        metadata: str | None,
        **kwargs,
    ):
        """Saves curation to tsv."""

    def _load_annotations(self, level: str) -> dict:
        """Load the annotations dictionary for a given level."""
        if level == "sample":
            return load_bson(get_annotations("sample"))

        if level == "series":
            return load_bson(get_annotations("series"))

        msg = f"Expected annotations level in {supported('levels')}, got {level}."
        if self.verbose:
            self.log.error(msg)
        raise ValueError(msg)

    def _parse_metafields(self, index_col: str, fields: str) -> list[str]:
        """Parse and check user-specified metadata fields."""
        _metadata = fields.split(",")

        flagged = False
        for field in _metadata:
            if field not in metadata_fields(index_col):
                flagged = True
                self.log.warning(
                    "Requested metadata: %s, is not available. Skipping...", field
                )

        if flagged:
            self.log.info("Run `metahq supported` to see available metadata fields.")

        if not index_col in _metadata:
            _metadata.append(index_col)
        return _metadata

    def _refinebio_in_metadata(self, metadata: list[str]) -> bool:
        """Checks if any refine.bio IDs are in requested metadata."""
        return len(list(set(metadata) & set(database_ids("refinebio")))) > 0

    def _sra_in_metadata(self, metadata: list[str]) -> bool:
        """Checks if any SRA IDs are in requested metadata."""
        return len(list(set(metadata) & set(database_ids("sra")))) > 0

    def _geo_fields_in_metadata(self, metadata: list[str], index_col: str) -> list[str]:
        """Returns the requested metadata fields that are sourced from the
        GEO metadata parquet (e.g. description, title, summary, ...).
        """
        return [field for field in metadata if field in geo_metadata_fields(index_col)]

    def _only_index(self, metadata: str | None, index: str) -> bool:
        """Check if no metadata passed or if only the index is passed."""
        return (metadata is None) or (
            isinstance(metadata, str) & (metadata.strip().replace(",", "") == index)
        )

    def _get_geo_metadata(self, curation: BaseCuration, fields: list[str]) -> pl.DataFrame:
        """Fetch requested GEO metadata fields (e.g. description, title,
        summary, source_name_ch1, ...) for a curation's index.
        """
        level = curation.index_col
        cols = self._geo_fields_in_metadata(fields, level)
        return (
            pl.scan_parquet(geo_metadata(level))
            .select([level, *cols])
            .filter(pl.col(level).is_in(curation.index))
            .collect()
        )

    def _get_save_method(self, fmt: str):
        """Returns appropriate saving method."""
        opt = {
            "parquet": self._save_parquet,
            "csv": self._save_csv,
            "tsv": self._save_tsv,
        }
        if fmt in opt:
            return opt[fmt]

        msg = f"Expected fmt in {list(opt.keys())}, got {fmt}."
        if self.verbose:
            self.log.error(msg)
        raise ValueError(msg)

    def _save_table_with_geo_metadata(
        self,
        file: FilePath,
        curation: BaseCuration,
        metadata: list[str],
        fmt: str,
        **kwargs,
    ):
        """Fetches requested GEO metadata fields and saves the curation in
        tabular format (parquet, csv, tsv).
        """
        geo_fields = self._geo_fields_in_metadata(metadata, curation.index_col)
        geo = self._get_geo_metadata(curation, geo_fields)
        ids = [field for field in metadata if field not in geo_fields]
        reorder = metadata + curation.entities

        df = (
            curation.ids.select(ids)
            .hstack(curation.data)
            .join(geo, on=curation.index_col, how="left")
            .select(reorder)
            .sort(curation.index_col)
        )

        save_method = self._get_save_method(fmt)
        save_method(df, file, **kwargs)

    def _save_parquet(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to parquet."""
        df.write_parquet(file, **kwargs)

    def _save_csv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator=",")

    def _save_tsv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator="\t")
