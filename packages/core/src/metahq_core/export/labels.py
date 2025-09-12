"""
Class for Labels export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-09-12 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.export.base import BaseExporter
from metahq_core.util.io import save_json
from metahq_core.util.supported import geo_metadata

if TYPE_CHECKING:
    from metahq_core.curations.labels import Labels
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


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
        # temp index
        stacked = curation.data.hstack(curation.ids)
        _labels: dict[str, list[str] | dict[str, str]] = {}

        if isinstance(metadata, str):
            _metadata = self._parse_metafields(curation.index_col, metadata)

            if "description" in _metadata:
                descs = self._get_descriptions(curation)
                stacked = stacked.join(descs, on=curation.index_col, how="left").sort(
                    curation.index_col
                )

            for col in curation.entities:
                _labels.setdefault(col, {})
                subset = stacked.filter(pl.col(col) == 1)[_metadata]

                for row in subset.iter_rows(named=True):
                    idx = row[curation.index_col]
                    _labels[col].setdefault(idx, {})
                    for additional in [i for i in _metadata if i != curation.index_col]:
                        _labels[col][idx][additional] = row[additional]

        else:
            for col in curation.entities:
                _labels[col] = stacked.filter(pl.col(col) == 1)[
                    curation.index_col
                ].to_list()

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

        print("SAVING WITH DESC")
        print(
            (
                labels.ids.select(ids)
                .hstack(labels.data)  # stack IDs with labels
                .join(desc, on=labels.index_col, how="left")  # join with desc
                .select(reorder)
                .sort(labels.index_col)
            ),
        )

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
            print(labels.ids.select(_metadata).sort("index"))
            print(labels.data)
            print(labels.filter(pl.col("MONDO:0005267") == 1))
            print(_metadata)
            print(
                labels.ids.select(_metadata).hstack(labels.data).sort(labels.index_col)
            )
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
