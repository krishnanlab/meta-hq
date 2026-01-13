"""
Class for Labels export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-11-21 by Parker Hicks
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
    disease_ontologies,
    geo_metadata,
    get_annotations,
    metadata_fields,
    supported,
)

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.labels import Labels
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


LABEL_KEY = {"1": "positive", "-1": "negative", "2": "control"}


class LabelsExporter(BaseExporter):
    """Base abstract class for Exporter children."""

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

    def add_sources(self, labels: Labels) -> Labels:
        """Add the sources that contributed to the lables of each sample or dataset.

        Arguments:
            labels (Labels):
                A populated Labels curation object.

        Returns:
            The Labels object with additional source IDs for each index.

        """
        sources = {labels.index_col: [], "sources": []}
        for idx in labels.index:
            sources[labels.index_col].append(idx)

            # get sources for a particular index for the specified attribute
            sources["sources"].append(
                "|".join(list(self._database[idx][self.attribute].keys()))
            )

        return labels.add_ids(pl.DataFrame(sources))

    def get_sra(self, labels: Labels, fields: list[str]) -> Labels:
        """
        Retrieve SRA IDs from the annotations if they exist.

        Parameters
        ----------
        labels: Labels
            A Labels curation containing samples and terms matching user-specified
            filters.

        fields: list[str]
            SRA ID levels (i.e., srr, srx, srs, or srp)

        Returns
        -------
        A new Annotations curation with merged SRA IDs.

        """
        _labels = self._load_annotations(
            level=labels.index_col
        )  # all MetaHQ annotations

        new_ids = {field: [] for field in fields}
        new_ids[labels.index_col] = []
        for idx in labels.index:
            new_ids[labels.index_col].append(idx)

            idx_accessions = _labels[idx]["accession_ids"]
            for field in fields:
                if field not in idx_accessions:
                    new_ids[field].append("NA")
                    continue

                new_ids[field].append(idx_accessions[field])

        return labels.add_ids(pl.DataFrame(new_ids))

    def save(
        self,
        labels: Labels,
        fmt: Literal["json", "parquet", "csv", "tsv"],
        file: FilePath,
        metadata: str | None = None,
        **kwargs,
    ):
        """

        Save labels curation to json. Keys are terms and values are
        positively labelstated indices.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.json.

        metadata: str
            Metadata fields to include.
        """
        _ = checkdir(file, is_file=True)
        opt = {
            "json": self.to_json,
            "parquet": self.to_parquet,
            "csv": self.to_csv,
            "tsv": self.to_tsv,
        }
        opt[fmt](labels, file, metadata, **kwargs)

        if self.verbose:
            self.log.info("Saved!")

    def to_csv(
        self, curation: Labels, file: FilePath, metadata: str | None = None, **kwargs
    ):
        """
        Save labels to csv.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.csv.

        metadata: str
            Metadata fields to include.

        """
        self._save_tabular("csv", curation, file, metadata, **kwargs)

    def to_json(self, curation: Labels, file: FilePath, metadata: str | None = None):
        """
        Save labels curation to json. Keys are terms and values are
        positively labelstated indices.

        Parameters
        ----------
        file: FilePath
            Path to outfile.json.

        metadata: str
            Metadata fields to include.

        """
        has_controls = any(
            term.startswith(disease_ontologies()) for term in curation.entities
        )
        if has_controls:
            _labels = {
                term: {"positive": [], "negative": [], "control": []}
                for term in curation.entities
            }
        else:
            _labels = {
                term: {"positive": [], "negative": []} for term in curation.entities
            }

        if (metadata is None) or (
            isinstance(metadata, str)
            & (metadata.strip().replace(",", "") == curation.index_col)
        ):
            metadata = curation.index_col

        if isinstance(metadata, str):
            # add sources
            curation = self.add_sources(curation)

            _metadata = self._parse_metafields(curation.index_col, metadata)
            _metadata.extend(["sources"])

            if self._sra_in_metadata(_metadata):
                curation = self.get_sra(
                    curation,
                    [field for field in _metadata if field in database_ids("sra")],
                )

            stacked = curation.data.hstack(curation.ids)

            if "description" in _metadata:
                descs = self._get_descriptions(curation)
                stacked = stacked.join(descs, on=curation.index_col, how="left").sort(
                    curation.index_col
                )

            for row in stacked.iter_rows(named=True):
                self._write_row_with_metadata(
                    row, _labels, curation.index_col, _metadata
                )
        else:
            msg = ("Unexpected metedata arguments %s", metadata)
            self.log.error(msg)
            self.log.debug("metadata dtype: %s", type(metadata))
            raise ValueError(msg)

        save_json(_labels, file)

    def to_numpy(self, curation: Labels) -> NpIntMatrix:
        """Returns the labelstation data as a numpy array."""
        return curation.data.to_numpy()

    def to_parquet(
        self,
        curation: Labels,
        file: FilePath,
        metadata: str | None = None,
        **kwargs,
    ):
        """
        Save labels to parquet.

        Parameters
        ----------
        curation: Labels
            Labels curation object to save.

        file: FilePath
            Path to outfile.parquet.

        metadata: str | None
            Metadata fields to include.

        """
        self._save_tabular("parquet", curation, file, metadata, **kwargs)

    def to_tsv(
        self, curation: Labels, file: FilePath, metadata: str | None = None, **kwargs
    ):
        """
        Save labels to tsv.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.tsv.

        metadata: str
            Metadata fields to include.

        """
        self._save_tabular("tsv", curation, file, metadata, **kwargs)

    def _get_descriptions(self, labels: Labels):
        """Collect descriptions to add the final output."""
        representative = labels.ids.row(0, named=True)[labels.index_col]
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
            .filter(pl.col(level).is_in(labels.index))
            .rename({level: labels.index_col})
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
        self, file: FilePath, labels: Labels, metadata: list[str], fmt: str, **kwargs
    ):
        """
        Fetches corresponding sample/study descriptions and saves the labels
        curation in tabular format (parquet, csv, tsv).
        """

        desc = self._get_descriptions(labels)
        ids = [m for m in metadata if m != "description"]
        reorder = metadata + labels.entities

        save_method = self._get_save_method(fmt)
        save_method(
            (
                labels.ids.select(ids)
                .hstack(labels.data)  # stack IDs with labels
                .join(desc, on=labels.index_col, how="left")  # join with desc
                .select(reorder)
                .sort(labels.index_col)
            ),
            file,
            **kwargs,
        )

    def _save_tabular(
        self,
        fmt: str,
        curation: Labels,
        file: FilePath,
        metadata: str | None = None,
        **kwargs,
    ):
        if isinstance(metadata, str):
            _metadata = self._parse_metafields(curation.index_col, metadata)

        else:
            _metadata = [curation.index_col]

        if self._sra_in_metadata(_metadata):
            curation = self.get_sra(
                curation, [field for field in _metadata if field in database_ids("sra")]
            )

        # add sources
        curation = self.add_sources(curation)
        _metadata = _metadata + ["sources"]

        if "description" in _metadata:
            self._save_table_with_description(
                file, curation, _metadata, fmt=fmt, **kwargs
            )

        else:
            self._get_save_method(fmt)(
                curation.ids.select(_metadata)
                .hstack(curation.data)
                .sort(curation.index_col),
                file,
                **kwargs,
            )

    def _save_parquet(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to parquet."""
        df.write_parquet(file, **kwargs)

    def _save_csv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator=",")

    def _save_tsv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator="\t")

    def _sra_in_metadata(self, metadata: list[str]) -> bool:
        """Checks if any SRA IDs are in requested metadata."""
        return len(list(set(metadata) & set(database_ids("sra")))) > 0

    def _write_row(self, row: dict[str, str], labels: dict[str, dict], index_col: str):
        """Write a row of an Annotations curation to a dictionary."""
        idx = row[index_col]
        for entity in labels:
            label = str(row[entity])
            if label in LABEL_KEY:
                labels[entity][LABEL_KEY[label]].append(idx)

    def _write_row_with_metadata(
        self,
        row: dict[str, str],
        labels: dict[str, dict],
        index_col: str,
        metadata: list[str],
    ):
        """Write a row of an Annotations curation to a dictionary with metadata."""
        idx = row[index_col]
        for entity in labels:
            label = str(row[entity])

            if label not in LABEL_KEY:
                continue

            # add sample with metadata
            cls = LABEL_KEY[label]
            idx_metadata = {idx: {}}
            for additional in [i for i in metadata if i != index_col]:
                idx_metadata[idx][additional] = row[additional]

            labels[entity][cls].append(idx_metadata)
