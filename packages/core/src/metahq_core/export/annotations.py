"""
Class for Annotations export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-09-08 by Parker Hicks
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from metahq_core.export.base import BaseExporter
from metahq_core.util.io import save_json

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


class AnnotationsExporter(BaseExporter):
    """Base abstract class for Exporter children."""

    def __init__(self, anno):
        self.anno: Annotations = anno

    def to_csv(self, file: FilePath, metadata: str | None = None, **kwargs):
        """
        Save annotations to csv.

        Parameters
        ----------
        outfile: FilePath
            Path to outfile.csv.

        metadata: bool
            If True, will add index titles to each entry.

        """
        self.anno.ids.hstack(self.data).write_csv(file, **kwargs)

    def to_json(self, file: FilePath, metadata: str | None = None):
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
        self.data = self.data.hstack(self.anno.ids)
        anno: dict[str, list[str] | dict[str, str]] = {}

        if isinstance(metadata, str):
            _metadata = metadata.split(",")
            if not self.anno.index_col in _metadata:
                _metadata.append(self.anno.index_col)

            for col in self.anno.entities:
                anno.setdefault(col, {})
                subset = self.data.filter(pl.col(col) == 1)[_metadata]

                for row in subset.iter_rows(named=True):
                    idx = row[self.anno.index_col]
                    anno[col].setdefault(idx, {})
                    for additional in [
                        i for i in _metadata if i != self.anno.index_col
                    ]:
                        anno[col][idx][additional] = row[additional]

        else:
            for col in self.anno.entities:
                anno[col] = self.data.filter(pl.col(col) == 1)[
                    self.anno.index_col
                ].to_list()

        save_json(anno, file)

    def to_numpy(self) -> NpIntMatrix:
        """Returns the annotation data as a numpy array."""
        return self.anno.data.to_numpy()

    def to_parquet(self, file: FilePath, metadata: str | None = None, **kwargs):
        """Save annotations to parquet file."""
        self.anno.ids.hstack(self.anno.data).write_parquet(file, **kwargs)

    def to_tsv(self, file: FilePath, metadata: str | None = None, **kwargs):
        """
        Save annotations to tsv.
        Parameters
        ----------
        outfile: FilePath
            Path to outfile.tsv.

        metadata: bool
            If True, will add index titles to each entry.

        """
        self.anno.ids.hstack(self.data).write_csv(file, separator="\t", **kwargs)
