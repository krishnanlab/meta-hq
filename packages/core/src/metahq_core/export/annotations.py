"""
Class for Annotations export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-09-08 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import polars as pl

from metahq_core.export.base import BaseExporter
from metahq_core.util.io import save_json

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


class AnnotationsExporter(BaseExporter):
    """Base abstract class for Exporter children."""

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
        anno.ids.hstack(anno.data).write_csv(file, **kwargs)

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
        stacked = anno.data.hstack(anno.ids)
        _anno: dict[str, list[str] | dict[str, str]] = {}

        if isinstance(metadata, str):
            _metadata = metadata.split(",")
            if not anno.index_col in _metadata:
                _metadata.append(anno.index_col)

            for col in anno.entities:
                _anno.setdefault(col, {})
                subset = stacked.filter(pl.col(col) == 1)[_metadata]

                for row in subset.iter_rows(named=True):
                    idx = row[anno.index_col]
                    _anno[col].setdefault(idx, {})
                    for additional in [i for i in _metadata if i != anno.index_col]:
                        _anno[col][idx][additional] = row[additional]

        else:
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
        outfile: FilePath
            Path to outfile.parquet.

        metadata: str
            Metadata fields to include.

        """
        anno.ids.hstack(anno.data).write_parquet(file, **kwargs)

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
        anno.ids.hstack(anno.data).write_csv(file, separator="\t", **kwargs)
