"""
Class for Annotations export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-09-23 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.export.base import BaseExporter
from metahq_core.util.io import checkdir, load_bson, save_json
from metahq_core.util.supported import (
    database_ids,
    geo_metadata,
    get_annotations,
    metadata_fields,
)

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


class AnnotationsExporter(BaseExporter):
    """Base abstract class for Exporter children."""

    def get_sra(self, anno: Annotations, fields: list[str]) -> Annotations:
        if anno.index_col == "sample":
            _anno = load_bson(get_annotations("sample"))
        elif anno.index_col == "series":
            _anno = load_bson(get_annotations("series"))
        else:
            raise ValueError(
                f"Expected index column name in [sample, series], got {anno.index_col}."
            )

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
        """

        Save annotations curation to json. Keys are terms and values are
        positively annotated indices.

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
        opt[fmt](anno, file, metadata, **kwargs)

    def to_csv(
        self, anno: Annotations, file: FilePath, metadata: str | None = None, **kwargs
    ):
        """
        Save annotations to csv.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.csv.

        metadata: str
            Metadata fields to include.

        """
        self._save_tabular("csv", anno, file, metadata, **kwargs)

    def to_json(self, anno: Annotations, file: FilePath, metadata: str | None = None):
        """
        Save annotations curation to json. Keys are terms and values are
        positively annotated indices.

        Parameters
        ----------
        file: FilePath
            Path to outfile.json.

        metadata: str
            Metadata fields to include.

        """
        # temp index
        _anno: dict[str, list[str] | dict[str, str]] = {}

        if isinstance(metadata, str):
            _metadata = self._parse_metafields(anno.index_col, metadata)

            if self._sra_in_metadata(_metadata):
                anno = self.get_sra(
                    anno, [field for field in _metadata if field in database_ids("sra")]
                )

            stacked = anno.data.hstack(anno.ids)

            if "description" in _metadata:
                descs = self._get_descriptions(anno)
                stacked = stacked.join(descs, on=anno.index_col, how="left")

            for col in anno.entities:
                _anno.setdefault(col, {})
                subset = stacked.filter(pl.col(col) == 1)[_metadata]

                for row in subset.iter_rows(named=True):
                    idx = row[anno.index_col]
                    _anno[col].setdefault(idx, {})
                    for additional in [i for i in _metadata if i != anno.index_col]:
                        _anno[col][idx][additional] = row[additional]

        else:
            stacked = anno.data.hstack(anno.ids)
            for col in anno.entities:
                _anno[col] = stacked.filter(pl.col(col) == 1)[anno.index_col].to_list()

        save_json(_anno, file)

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
        """
        Save annotations to parquet.

        Parameters
        ----------
        anno: Annotations
            Annotations curation object to save.

        file: FilePath
            Path to outfile.parquet.

        metadata: str | None
            Metadata fields to include.

        """
        self._save_tabular("parquet", anno, file, metadata, **kwargs)

    def to_tsv(
        self, anno: Annotations, file: FilePath, metadata: str | None = None, **kwargs
    ):
        """
        Save annotations to tsv.
        Parameters
        ----------
        outfile: FilePath
            Path to outfile.tsv.

        metadata: str
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
            raise RuntimeError(
                "Congradulations! You broke the application. Please submit an issue."
            )

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

        raise ValueError(f"Expected fmt in {list(opt.keys())}, got {fmt}.")

    def _parse_metafields(self, index_col, fields: str) -> list[str]:
        """Parse and check user-specified metadata fields."""
        _metadata = fields.split(",")

        flagged = False
        for field in _metadata:
            if field not in metadata_fields(index_col):
                flagged = True
                print(f"Requested metadata: {field}, is not available. Skipping...")

        if flagged:
            print("Run metahq metadata to see available metadata fields.")

        if not index_col in _metadata:
            _metadata.append(index_col)
        return _metadata

    def _save_table_with_description(
        self, file: FilePath, anno: Annotations, metadata: list[str], fmt: str, **kwargs
    ):
        """
        Fetches corresponding sample/study descriptions and saves the annotations
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

        if "description" in _metadata:
            self._save_table_with_description(file, anno, _metadata, fmt=fmt, **kwargs)

        else:
            self._get_save_method(fmt)(
                anno.ids.select(_metadata).hstack(anno.data), file, **kwargs
            )

    def _sra_in_metadata(self, metadata: list[str]) -> bool:
        """Checks if any SRA IDs are in requested metadata."""
        return len(list(set(metadata) & set(database_ids("sra")))) > 0

    def _save_parquet(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to parquet."""
        df.write_parquet(file, **kwargs)

    def _save_csv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator=",")

    def _save_tsv(self, df: pl.DataFrame, file: FilePath, **kwargs):
        """Save polars DataFrame to csv/tsv."""
        df.write_csv(file, **kwargs, separator="\t")
