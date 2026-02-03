"""
Class for Annotations export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2026-02-03 by Parker Hicks
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.export.base import BaseExporter
from metahq_core.logger import setup_logger
from metahq_core.util.io import checkdir, load_bson, save_json
from metahq_core.util.supported import (
    database_ids,
    geo_metadata,
    get_annotations,
    metadata_fields,
    supported,
)

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
        logdir=Path("."),
        verbose=True,
    ):
        self.attribute = attribute
        self._database = self._load_annotations(level)

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def add_sources(self, anno: Annotations) -> Annotations:
        """Add the sources that contributed to the lables of each sample or dataset.

        Arguments:
            anno (Annotations):
                A populated Labels curation object.

        Returns:
            The Labels object with additional source IDs for each index.

        """
        sources = {anno.index_col: [], "sources": []}
        for idx in anno.index:
            sources[anno.index_col].append(idx)

            # get sources for a particular index for the specified attribute
            sources["sources"].append(
                "|".join(list(self._database[idx][self.attribute].keys()))
            )

        return anno.add_ids(pl.DataFrame(sources))

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

        opt[fmt](anno, file, metadata, **kwargs)

        if self.verbose:
            self.log.info("Saved!")

    def to_csv(
        self, anno: Annotations, file: FilePath, metadata: str | None = None, **kwargs
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
        self._save_tabular("csv", anno, file, metadata, **kwargs)

    def to_json(self, anno: Annotations, file: FilePath, metadata: str | None = None):
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
            self._save_json_with_metadata(anno, file, anno.index_col)

        elif isinstance(metadata, str):
            self._save_json_with_metadata(anno, file, metadata)

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
        self._save_tabular("parquet", anno, file, metadata, **kwargs)

    def to_tsv(
        self, anno: Annotations, file: FilePath, metadata: str | None = None, **kwargs
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
        self._save_tabular("tsv", anno, file, metadata, **kwargs)

    def _get_descriptions(self, anno: Annotations):
        """Collect descriptions to add the final output."""
        representative = anno.ids.row(0, named=True)[anno.index_col]
        if representative.startswith("GSM"):
            level = "sample"
        elif representative.startswith("GSE"):
            level = "series"
        else:
            msg = "Congratulations! You broke the application. Please submit an issue."
            if self.verbose:
                self.log.error(msg)
                self.log.debug(
                    "%s was used to identify if the passed level is sample or series",
                    representative,
                )
            raise RuntimeError(msg)

        return (
            pl.scan_parquet(geo_metadata(level))
            .select([level, "description"])
            .filter(pl.col(level).is_in(anno.index))
            .rename({level: anno.index_col})
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

        msg = ("Expected fmt in %s, got %s.", list(opt.keys()), fmt)
        if self.verbose:
            self.log.error(msg)
        raise ValueError(msg)

    def _load_annotations(self, level: str) -> dict:
        """Load the annotations dictionary for a given level."""
        if level == "sample":
            return load_bson(get_annotations("sample"))

        if level == "series":
            return load_bson(get_annotations("series"))

        msg = ("Expected annotations level in %s, got %s.", supported("levels"), level)
        if self.verbose:
            self.log.error(msg)
        raise ValueError(msg)

    def _parse_metafields(self, index_col, fields: str) -> list[str]:
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

    def _save_table_with_description(
        self, file: FilePath, anno: Annotations, metadata: list[str], fmt: str, **kwargs
    ):
        """Fetches corresponding sample/study descriptions and saves the annotations
        curation in tabular format (parquet, csv, tsv).
        """

        desc = self._get_descriptions(anno)
        ids = [m for m in metadata if m != "description"]
        reorder = metadata + anno.entities

        save_method = self._get_save_method(fmt)
        save_method(
            (
                anno.ids.select(ids)
                .hstack(anno.data)  # stack IDs with annotations
                .join(desc, on=anno.index_col, how="left")  # join with desc
                .select(reorder)
            ),
            file,
            **kwargs,
        )

    def _save_tabular(
        self,
        fmt: str,
        anno: Annotations,
        file: FilePath,
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

        # add sources
        anno = self.add_sources(anno)
        _metadata.extend(["sources"])

        if "description" in _metadata:
            self._save_table_with_description(file, anno, _metadata, fmt=fmt, **kwargs)

        else:
            self._get_save_method(fmt)(
                anno.ids.select(_metadata).hstack(anno.data), file, **kwargs
            )

    def _sra_in_metadata(self, metadata: list[str]) -> bool:
        """Checks if any SRA IDs are in requested metadata."""
        return len(list(set(metadata) & set(database_ids("sra")))) > 0

    def _only_index(self, metadata: str | None, index: str):
        """Check if no metadata passed or if only the index is passed."""
        return (metadata is None) or (
            isinstance(metadata, str) & (metadata.strip().replace(",", "") == index)
        )

    def _save_parquet(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to parquet."""
        df.write_parquet(file, **kwargs)

    def _save_csv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator=",")

    def _save_json_only_index(self, anno: Annotations, file: FilePath):
        """Save annotations as JSON with only the index."""
        _anno: dict[str, list[str]] = {}
        stacked = anno.data.hstack(anno.ids)
        for col in anno.entities:
            _anno[col] = stacked.filter(pl.col(col) == 1)[anno.index_col].to_list()

        save_json(_anno, file)

    def _save_json_with_metadata(
        self, anno: Annotations, file: FilePath, metadata: str
    ):
        """Save annotations as JSON with requested metadata."""

        # add sources
        anno = self.add_sources(anno)

        _anno: dict[str, dict[str, dict[str, str]]] = {
            term: {} for term in anno.entities
        }
        _metadata = self._parse_metafields(anno.index_col, metadata)
        _metadata.extend(["sources"])

        if self._sra_in_metadata(_metadata):
            anno = self.get_sra(
                anno, [field for field in _metadata if field in database_ids("sra")]
            )

        stacked = anno.data.hstack(anno.ids)

        if "description" in _metadata:
            descs = self._get_descriptions(anno)
            stacked = stacked.join(descs, on=anno.index_col, how="left").sort(
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

    def _save_tsv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator="\t")

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
