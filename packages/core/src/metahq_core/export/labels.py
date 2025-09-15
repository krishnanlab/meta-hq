"""
Class for Labels export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-09-12 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import polars as pl

from metahq_core.export.base import BaseExporter
from metahq_core.util.io import save_json
from metahq_core.util.supported import geo_metadata

if TYPE_CHECKING:
    from metahq_core.curations.labels import Labels
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


LABEL_KEY = {"1": "positive", "-1": "negative", "2": "control"}


class LabelsExporter(BaseExporter):
    """Base abstract class for Exporter children."""

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
        opt = {
            "json": self.to_json,
            "parquet": self.to_parquet,
            "csv": self.to_csv,
            "tsv": self.to_tsv,
        }
        opt[fmt](labels, file, metadata, **kwargs)

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
        stacked = curation.data.hstack(curation.ids)  # anno with IDs
        _labels = {
            term: {"positive": [], "negative": [], "control": []}
            for term in curation.entities
        }

        if metadata is None:
            # save with just index IDs
            for row in stacked.iter_rows(named=True):
                self._write_row(row, _labels, curation.index_col)

        elif isinstance(metadata, str) & (metadata.strip().replace(",", "") == "index"):
            # save with just index IDs
            for row in stacked.iter_rows(named=True):
                self._write_row(row, _labels, curation.index_col)

        elif isinstance(metadata, str):
            _metadata = self._parse_metafields(curation.index_col, metadata)

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
            raise ValueError("Weird metadata arguments.")

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
            raise RuntimeError(
                "Congradulations! You broke the application. Please submit an issue."
            )

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

        raise ValueError(f"Expected fmt in {list(opt.keys())}, got {fmt}.")

    def _parse_metafields(self, index_col, fields: str) -> list[str]:
        _metadata = fields.split(",")
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
        labels: Labels,
        file: FilePath,
        metadata: str | None = None,
        **kwargs,
    ):
        if isinstance(metadata, str):
            _metadata = self._parse_metafields(labels.index_col, metadata)

        else:
            _metadata = list(labels.index_col)

        if "description" in _metadata:
            self._save_table_with_description(
                file, labels, _metadata, fmt=fmt, **kwargs
            )

        else:
            self._get_save_method(fmt)(
                labels.ids.select(_metadata).hstack(labels.data).sort(labels.index_col),
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
