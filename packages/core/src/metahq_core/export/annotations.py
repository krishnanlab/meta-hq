"""
Class for Annotations export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2026-04-13 by Parker Hicks
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.config import SOURCES_COL
from metahq_core.export.base import BaseExporter
from metahq_core.export.refinebio import RefineBioExporter
from metahq_core.export.references import CitationConfig, save_citations
from metahq_core.logger import setup_logger
from metahq_core.util.io import checkdir, save_json
from metahq_core.util.supported import database_ids, get_default_log_dir

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.annotations import Annotations
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


ANNOTATION_KEY = {"1": True, "0": False}


class AnnotationsExporter(BaseExporter):
    """Exporter for Annotations curations.

    Attributes:
        attribute (Literal["tissue", "disease", "sex", "age"]):
            Attribute of the annotations to save.

        level (Literal["sample", "series"]):
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
        self._database = self._load_annotations(level)

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose
        self._refinebio = RefineBioExporter(logger=self.log, verbose=self.verbose)

    def get_sra(self, anno: Annotations, fields: list[str]) -> Annotations:
        """
        Retrieve SRA IDs from the annotations if they exist.

        Arguments:
            anno (Annotations):
                An Annotations curation containing samples and terms matching user-specified
                filters.

            fields (list[str]):
                SRA ID levels (i.e., srr, srx, srs, or srp)

        Returns:
            A new Annotations curation with merged SRA IDs.

        """
        _anno = self._load_annotations(level=anno.index_col)  # all MetaHQ annotations

        new_ids = {field: [] for field in fields}
        new_ids[anno.index_col] = []
        for idx in anno.index:
            new_ids[anno.index_col].append(idx)

            idx_accessions = _anno[idx]["accession_ids"]
            for field in fields:
                if field not in idx_accessions:
                    new_ids[field].append("NA")
                    continue

                new_ids[field].append(idx_accessions[field])

        return anno.add_ids(pl.DataFrame(new_ids))

    def save(
        self,
        anno: Annotations,
        fmt: Literal["json", "parquet", "csv", "tsv"],
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save annotations curation to json. Keys are terms and values are
        positively annotated indices.

        Arguments:
            anno (Annotations):
                A populated Annotations object.

            fmt (Literal["json", "parquet", "csv", "tsv"]):
                File format to save to.

            file (FilePath):
                Path to outfile.json.

            citation_config (CitationConfig):
                Parameters for saving citations.

            metadata (str):
                Metadata fields to include.

        """
        _ = checkdir(file, is_file=True)
        opt = {
            "json": self.to_json,
            "parquet": self.to_parquet,
            "csv": self.to_csv,
            "tsv": self.to_tsv,
        }

        opt[fmt](anno, file, citation_config, metadata, **kwargs)

        if self.verbose:
            self.log.info("Saved!")

    def to_refinebio_dataset(self, anno: Annotations) -> dict:
        """Create a pre-populated refine.bio dataset from this curation's
        samples and series, and submit it through refine.bio's dataset API.

        Arguments:
            anno (Annotations):
                A populated Annotations curation.

        Returns:
            The JSON response from refine.bio's dataset API.
        """
        return self._refinebio.create_dataset(anno)

    def to_csv(
        self,
        anno: Annotations,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save annotations to csv.

        Arguments:
            anno (Annotations):
                A populated Annotations object.

            file (FilePath):
                Path to outfile.csv.

            metadata (str):
                Metadata fields to include.

        """
        self._save_tabular("csv", anno, file, citation_config, metadata, **kwargs)

    def to_json(
        self,
        anno: Annotations,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
    ):
        """Save annotations curation to json. Keys are terms and values are
        positively annotated indices.

        Arguments:
            anno (Annotations):
                A populated Annotations object.

            file (FilePath):
                Path to outfile.json.

            metadata (str):
                Metadata fields to include.

        """

        if self._only_index(metadata, anno.index_col):
            self._save_json_with_metadata(anno, file, citation_config, anno.index_col)

        elif isinstance(metadata, str):
            self._save_json_with_metadata(anno, file, citation_config, metadata)

        else:
            msg = ("Unexpected metedata arguments %s", metadata)
            self.log.error(msg)
            self.log.debug("metadata dtype: %s", type(metadata))
            raise ValueError(msg)

    def to_numpy(self, anno: Annotations) -> NpIntMatrix:
        """Returns the annotation data as a numpy array."""
        return anno.data.to_numpy()

    def to_parquet(
        self,
        anno: Annotations,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save annotations to parquet.

        Arguments:
            anno (Annotations):
                Annotations curation object to save.

            file (FilePath):
                Path to outfile.parquet.

            metadata (str | None):
                Metadata fields to include.

        """
        self._save_tabular("parquet", anno, file, citation_config, metadata, **kwargs)

    def to_tsv(
        self,
        anno: Annotations,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save annotations to tsv.

        Arguments:
            anno (Annotations):
                A populated Annotations object.

            file (FilePath):
                Path to outfile.tsv.

            metadata (str):
                Metadata fields to include.

        """
        self._save_tabular("tsv", anno, file, citation_config, metadata, **kwargs)

    def _save_tabular(
        self,
        fmt: str,
        anno: Annotations,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        if isinstance(metadata, str):
            _metadata = self._parse_metafields(anno.index_col, metadata)

        else:
            _metadata = [anno.index_col]

        if self._sra_in_metadata(_metadata):
            anno = self.get_sra(
                anno, [field for field in _metadata if field in database_ids("sra")]
            )

        if self._refinebio_in_metadata(_metadata):
            anno = self._refinebio.get_refinebio(
                anno,
                [field for field in _metadata if field in database_ids("refinebio")],
            )

        _metadata.extend([SOURCES_COL])

        # save sources to citation file
        save_citations(
            anno.ids[SOURCES_COL].str.split("|").explode().value_counts(sort=True),
            citation_config,
            logger=self.log,
            verbose=self.verbose,
        )

        self.log.info("Saving retrieval result to %s", Path(file).parent)
        if self._geo_fields_in_metadata(_metadata, anno.index_col):
            self._save_table_with_geo_metadata(file, anno, _metadata, fmt=fmt, **kwargs)

        else:
            self._get_save_method(fmt)(
                anno.ids.select(_metadata).hstack(anno.data), file, **kwargs
            )

    def _save_json_only_index(self, anno: Annotations, file: FilePath):
        """Save annotations as JSON with only the index."""
        self.log.info("Saving retrieval result to %s", file)
        _anno: dict[str, list[str]] = {}
        stacked = anno.data.hstack(anno.ids)
        for col in anno.entities:
            _anno[col] = stacked.filter(pl.col(col) == 1)[anno.index_col].to_list()

        save_json(_anno, file)

    def _save_json_with_metadata(
        self,
        anno: Annotations,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str,
    ):
        """Save annotations as JSON with requested metadata."""

        save_citations(
            anno.ids[SOURCES_COL].str.split("|").explode().value_counts(sort=True),
            citation_config,
            logger=self.log,
        )

        self.log.info("Saving retrieval result to %s", Path(file).parent)
        _anno: dict[str, dict[str, dict[str, str]]] = {
            term: {} for term in anno.entities
        }
        _metadata = self._parse_metafields(anno.index_col, metadata)
        _metadata.extend(["sources"])

        if self._sra_in_metadata(_metadata):
            anno = self.get_sra(
                anno, [field for field in _metadata if field in database_ids("sra")]
            )

        if self._refinebio_in_metadata(_metadata):
            anno = self._refinebio.get_refinebio(
                anno,
                [field for field in _metadata if field in database_ids("refinebio")],
            )

        stacked = anno.data.hstack(anno.ids)

        geo_fields = self._geo_fields_in_metadata(_metadata, anno.index_col)
        if geo_fields:
            geo = self._get_geo_metadata(anno, geo_fields)
            stacked = stacked.join(geo, on=anno.index_col, how="left").sort(
                anno.index_col
            )

        for col in anno.entities:
            _anno.setdefault(col, {})
            subset = stacked.filter(pl.col(col) == 1)[_metadata]

            for row in subset.iter_rows(named=True):
                self._write_row_with_metadata(
                    row, anno.index_col, _anno, col, _metadata
                )

        save_json(_anno, file)

    def _write_row(
        self, row: dict[str, str], anno: dict[str, list[str]], index_col: str
    ):
        """Write a row of an Annotations curation to a dictionary."""
        idx = row[index_col]
        for entity in anno:
            _anno = str(row[entity])
            if _anno in ANNOTATION_KEY:
                anno[entity].append(idx)

    def _write_row_with_metadata(
        self,
        row: dict[str, str],
        index: str,
        anno: dict[str, dict],
        entity: str,
        metadata: list[str],
    ):
        """Write a row of an Annotations curation to a dictionary with metadata."""
        idx = row[index]
        anno[entity].setdefault(idx, {})
        for additional in [i for i in metadata if i != index]:
            anno[entity][idx][additional] = row[additional]
