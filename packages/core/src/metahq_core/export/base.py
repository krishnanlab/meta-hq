"""
Abstract base class for Curation export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2026-08-12 by Parker Hicks
"""

from __future__ import annotations

import json
from abc import ABC
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.config import EXTERNAL_LINKS_COL, SOURCES_COL
from metahq_core.export.references import save_citations
from metahq_core.export.refinebio import RefineBioExporter
from metahq_core.logger import setup_logger
from metahq_core.util.alltypes import (
    LEVEL_TO_FIELDS,
    LEVEL_TO_INDEX_FIELD,
    Level,
    MetadataField,
)
from metahq_core.util.io import checkdir, load_bson
from metahq_core.util.supported import (
    database_ids,
    geo_metadata,
    geo_metadata_fields,
    get_annotations,
    get_default_log_dir,
    get_external_links,
    sources_with_external_links,
    supported,
)

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.base import BaseCuration
    from metahq_core.export.references import CitationConfig
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


class BaseExporter(ABC):
    """Base abstract class for Exporter children.

    Attributes:
        attribute (str):
            Attribute of the annotations to save.

        level (str):
            Level of the annotations.

        logger (logging.Logger):
            Python builtin Logger.

        loglevel (int):
            Logging level.

        logdir (str | Path):
            Path to directory storing logs.

        verbose (bool):
            Controls logging outputs.
    """

    def __init__(
        self,
        attribute: str,
        level: str,
        logger=None,
        loglevel=20,
        logdir=get_default_log_dir(),
        verbose=True,
    ):
        self.attribute = attribute
        self._level = Level(level)
        self._database = self._load_annotations()

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose
        self._refinebio = RefineBioExporter(logger=self.log, verbose=self.verbose)

    def add_external_links(self, curation: BaseCuration) -> BaseCuration:
        """Attaches external links to a curation."""
        external_links = self._add_external_links(
            self._extract_sources_for_links(curation)
        )

        match self._level:
            case Level.SAMPLE:
                return curation.add_ids_on_group(external_links, on="series")

            case Level.SERIES:
                return curation.add_ids_partial(external_links)

            case _:
                raise ValueError(
                    f"Expected level in [sample, series]. Got {self._level}."
                )

    def to_numpy(self, curation: BaseCuration) -> NpIntMatrix:
        """Returns curation's data matrix as a numpy array."""
        return curation.data.to_numpy()

    def to_parquet(
        self,
        curation: BaseCuration,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save curation to parquet.

        Arguments:
            curation (BaseCuration):
                A populated curation object.

            file (FilePath):
                Path to outfile.parquet.

            citation_config (CitationConfig):
                Parameters for saving citations.

            metadata (str | None):
                Metadata fields to include.
        """
        self._save_tabular(
            "parquet", curation, file, citation_config, metadata, **kwargs
        )

    def to_csv(
        self,
        curation: BaseCuration,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save curation to csv.

        Arguments:
            curation (BaseCuration):
                A populated curation object.

            file (FilePath):
                Path to outfile.csv.

            citation_config (CitationConfig):
                Parameters for saving citations.

            metadata (str | None):
                Metadata fields to include.
        """
        self._save_tabular("csv", curation, file, citation_config, metadata, **kwargs)

    def to_tsv(
        self,
        curation: BaseCuration,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save curation to tsv.

        Arguments:
            curation (BaseCuration):
                A populated curation object.

            file (FilePath):
                Path to outfile.tsv.

            citation_config (CitationConfig):
                Parameters for saving citations.

            metadata (str | None):
                Metadata fields to include.
        """
        self._save_tabular("tsv", curation, file, citation_config, metadata, **kwargs)

    def get_sra(self, curation: BaseCuration, fields: list[str]) -> BaseCuration:
        """Retrieve SRA IDs from the MetaHQ annotations database if they exist.

        Arguments:
            curation (BaseCuration):
                A curation containing samples or series matching user-specified
                filters.

            fields (list[str]):
                SRA ID levels (i.e., srr, srx, srs, or srp).

        Returns:
            A new curation with merged SRA IDs.
        """
        _database = self._load_annotations()

        new_ids = {field: [] for field in fields}
        new_ids[curation.index_col] = []
        for idx in curation.index:
            new_ids[curation.index_col].append(idx)

            idx_accessions = _database[idx]["accession_ids"]
            for field in fields:
                if field not in idx_accessions:
                    new_ids[field].append("NA")
                    continue

                new_ids[field].append(idx_accessions[field])

        return curation.add_ids(pl.DataFrame(new_ids))

    def save(
        self,
        curation: BaseCuration,
        fmt: Literal["json", "parquet", "csv", "tsv"],
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save a curation to file.

        Arguments:
            curation (BaseCuration):
                A populated curation object.

            fmt (Literal["json", "parquet", "csv", "tsv"]):
                File format to save to.

            file (FilePath):
                Path to outfile.

            citation_config (CitationConfig):
                Parameters for saving citations.

            metadata (str | None):
                Metadata fields to include.
        """
        _ = checkdir(file, is_file=True)
        opt = {
            "json": self.to_json,
            "parquet": self.to_parquet,
            "csv": self.to_csv,
            "tsv": self.to_tsv,
        }

        opt[fmt](curation, file, citation_config, metadata, **kwargs)

        if self.verbose:
            self.log.info("Saved!")

    def to_refinebio_dataset(self, curation: BaseCuration) -> dict:
        """Create a pre-populated refine.bio dataset from this curation's
        samples and series, and submit it through refine.bio's dataset API.

        Arguments:
            curation (BaseCuration):
                A populated curation containing samples or series matching
                user-specified filters.

        Returns:
            The JSON response from refine.bio's dataset API.
        """
        return self._refinebio.create_dataset(curation)

    def _add_external_links(self, sources: pl.DataFrame) -> pl.DataFrame:
        """Adds an external links column to any series IDs.

        Arguments:
            sources (pl.DataFrame):
                A long data frame with columns [series, sources] where one row indicates a single series,
                source pair. A single series IDs can map to multiple sources.

        Returns:
            (pl.DataFrame): The sources data frame with an additional external_links column.
        """

        def to_json(structs: list[dict]) -> str:
            return json.dumps({s[SOURCES_COL]: s["link"] for s in structs})

        linked_sources = sources_with_external_links()
        try:
            links = (
                pl.scan_parquet(get_external_links())
                .unpivot(
                    on=linked_sources,
                    index="series",
                    variable_name=SOURCES_COL,
                    value_name="link",
                )
                .collect(engine="streaming")
            )
        except FileNotFoundError as e:
            if self.verbose:
                self.log.error(e)
            raise e

        sources = sources.filter(pl.col(SOURCES_COL).is_in(linked_sources)).join(
            links, on=["series", SOURCES_COL], how="left"
        )
        sources = (
            sources.group_by("series", maintain_order=True)
            .agg(pl.struct([SOURCES_COL, "link"]).alias("link_with_source"))
            .with_columns(
                pl.col("link_with_source")
                .list.eval(pl.element().struct.field(SOURCES_COL))
                .list.join("|")
                .alias(SOURCES_COL)
            )
            .drop(SOURCES_COL)
        )

        sources = sources.with_columns(
            pl.col("link_with_source")
            .map_elements(to_json, return_dtype=pl.String)
            .alias(EXTERNAL_LINKS_COL)
        ).drop("link_with_source")

        return sources

    def _extract_sources_for_links(self, curation: BaseCuration) -> pl.DataFrame:
        """Extracts sources and series information from the curation.

        Arguments:
            curation (BaseCuration):
                A populated Curation object with id columns [series, sources].

        Returns:
            (pl.DataFrame): A long data frame with columns [series, sources].
        """
        required_cols = ["series", SOURCES_COL]
        missing = []
        for col in required_cols:
            if col not in curation.ids:
                missing.append(col)

        if len(missing) > 0:
            msg = f"Missing columns {missing} in the retrieved curation"
            self.log.error(msg)
            raise ValueError(msg)

        return (
            curation.ids.select(required_cols)
            .with_columns(pl.col(SOURCES_COL).str.split("|"))
            .explode(SOURCES_COL)
            .unique()
            .with_columns(pl.col("series").str.split("|"))
            .explode("series")
            .unique()
        )

    def _load_annotations(self) -> dict:
        """Load the annotations dictionary for a given level."""
        match self._level:
            case Level.SAMPLE:
                return load_bson(get_annotations("sample"))

            case Level.SERIES:
                return load_bson(get_annotations("series"))

            case _:
                msg = f"Expected annotations level in {supported('levels')}, got {self._level}."
                if self.verbose:
                    self.log.error(msg)
                raise ValueError(msg)

    def _parse_metafields(self, level: Level | str, fields: str) -> list[MetadataField]:
        """Parse and check user-specified metadata fields."""
        _level = Level(level) if isinstance(level, str) else level
        allowed = LEVEL_TO_FIELDS[_level]
        requested = fields.split(",")

        resolved: list[MetadataField] = []
        flagged = False

        for raw in requested:
            raw = raw.strip()
            try:
                field = MetadataField(raw)
            except ValueError:
                field = None

            if field is None or field not in allowed:
                flagged = True
                self.log.warning(
                    "Requested metadata: %s, is not available. Skipping...", raw
                )
                continue

            resolved.append(field)

        if flagged:
            self.log.info("Run `metahq supported` to see available metadata fields.")

        index_field = LEVEL_TO_INDEX_FIELD[_level]
        if index_field not in resolved:
            resolved.append(index_field)

        return resolved

    def _refinebio_in_metadata(self, metadata: list[MetadataField]) -> bool:
        """Checks if any refine.bio IDs are in requested metadata."""
        _fields = [field.value for field in metadata]
        return len(list(set(_fields) & set(database_ids("refinebio")))) > 0

    def _sra_in_metadata(self, metadata: list[MetadataField]) -> bool:
        """Checks if any SRA IDs are in requested metadata."""
        _fields = [field.value for field in metadata]
        return len(list(set(_fields) & set(database_ids("sra")))) > 0

    def _geo_fields_in_metadata(
        self, metadata: list[MetadataField], index_col: str
    ) -> list[str]:
        """Returns the requested metadata fields that are sourced from the
        GEO metadata parquet (e.g. description, title, summary, ...).
        """
        _fields = [field.value for field in metadata]
        return [field for field in _fields if field in geo_metadata_fields(index_col)]

    def _only_index(self, metadata: str | None, index: str) -> bool:
        """Check if no metadata passed or if only the index is passed."""
        return (metadata is None) or (
            isinstance(metadata, str) and (metadata.strip().replace(",", "") == index)
        )

    def _get_geo_metadata(
        self, curation: BaseCuration, fields: list[str]
    ) -> pl.DataFrame:
        """Fetch requested GEO metadata fields (e.g. description, title,
        summary, source_name_ch1, ...) for a curation's index.
        """
        level = curation.index_col
        return (
            pl.scan_parquet(geo_metadata(level))
            .select([level, *fields])
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

    def _save_tabular(
        self,
        fmt: str,
        curation: BaseCuration,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Fetches SRA/refine.bio/GEO fields as requested and saves the
        curation in tabular format (parquet, csv, tsv), sorted by index.
        """
        if isinstance(metadata, str):
            _metadata = self._parse_metafields(curation.index_col, metadata)

        else:
            _metadata = [LEVEL_TO_INDEX_FIELD[Level(curation.index_col)]]

        if self._sra_in_metadata(_metadata):
            curation = self.get_sra(
                curation, [field.value for field in _metadata if field.value in database_ids("sra")]
            )

        if self._refinebio_in_metadata(_metadata):
            curation = self._refinebio.get_refinebio(
                curation,
                [field.value for field in _metadata if field.value in database_ids("refinebio")],
            )

        _metadata = _metadata + [MetadataField.SOURCES, MetadataField.EXTERNAL_LINKS]

        # add link to original sources where applicable
        curation = self.add_external_links(curation)

        # save sources to citation file
        save_citations(
            curation.ids[SOURCES_COL].str.split("|").explode().value_counts(sort=True),
            citation_config,
            logger=self.log,
            verbose=self.verbose,
        )

        self.log.info("Saving retrieval result to %s", Path(file).parent)
        if self._geo_fields_in_metadata(_metadata, curation.index_col):
            self._save_table_with_geo_metadata(
                file, curation, _metadata, fmt=fmt, **kwargs
            )

        else:
            self._get_save_method(fmt)(
                curation.ids.select([field.value for field in _metadata])
                .hstack(curation.data)
                .sort(curation.index_col),
                file,
                **kwargs,
            )

    def _save_table_with_geo_metadata(
        self,
        file: FilePath,
        curation: BaseCuration,
        metadata: list[MetadataField],
        fmt: str,
        **kwargs,
    ):
        """Fetches requested GEO metadata fields and saves the curation in
        tabular format (parquet, csv, tsv).
        """
        geo_fields = self._geo_fields_in_metadata(metadata, curation.index_col)
        geo = self._get_geo_metadata(curation, geo_fields)
        metadata_str = [field.value for field in metadata]
        ids = [field for field in metadata_str if field not in geo_fields]
        reorder = metadata_str + curation.entities

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
