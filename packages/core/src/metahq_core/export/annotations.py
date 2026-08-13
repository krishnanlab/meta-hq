"""
Class for Annotations export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2026-08-13 by Parker Hicks
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from metahq_core.config import EXTERNAL_LINKS_COL, SOURCES_COL
from metahq_core.export.base import BaseExporter
from metahq_core.export.references import save_citations
from metahq_core.util.alltypes import MetadataField
from metahq_core.util.io import save_json
from metahq_core.util.supported import database_ids

if TYPE_CHECKING:
    from metahq_core.curations.annotations import Annotations
    from metahq_core.export.references import CitationConfig
    from metahq_core.util.alltypes import FilePath


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
            msg = f"Unexpected metadata argument: {metadata!r}"
            self.log.error(msg)
            self.log.debug("metadata dtype: %s", type(metadata))
            raise ValueError(msg)

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

        # initialize output dict
        _anno: dict[str, dict[str, dict[str, str]]] = {
            term: {} for term in anno.entities
        }
        _metadata = self._parse_metafields(anno.index_col, metadata)
        _metadata.extend([MetadataField.SOURCES, MetadataField.EXTERNAL_LINKS])

        # include external links
        anno = self.add_external_links(anno)

        # append sra IDs if requested
        if self._sra_in_metadata(_metadata):
            anno = self.get_sra(
                anno,
                [
                    field.value
                    for field in _metadata
                    if field.value in database_ids("sra")
                ],
            )

        # append refine.bio IDs if requested
        if self._refinebio_in_metadata(_metadata):
            anno = self._refinebio.get_refinebio(
                anno,
                [field.value for field in _metadata if field.value in database_ids("refinebio")],
            )

        # append requested GEO metadata fields
        stacked = anno.data.hstack(anno.ids)
        geo_fields = self._geo_fields_in_metadata(_metadata, anno.index_col)
        if geo_fields:
            geo = self._get_geo_metadata(anno, geo_fields)
            stacked = stacked.join(geo, on=anno.index_col, how="left").sort(
                anno.index_col
            )

        # convert data frame to dict
        for col in anno.entities:
            _anno.setdefault(col, {})
            subset = stacked.filter(pl.col(col) == 1)[[field.value for field in _metadata]]

            for row in subset.iter_rows(named=True):
                self._write_row_with_metadata(
                    row, anno.index_col, _anno, col, _metadata
                )

        save_json(_anno, file)

    def _write_row_with_metadata(
        self,
        row: dict[str, str],
        index: str,
        anno: dict[str, dict],
        entity: str,
        metadata: list[MetadataField],
    ):
        """Write a row of an Annotations curation to a dictionary with metadata."""
        idx = row[index]
        anno[entity].setdefault(idx, {})
        for additional in [i for i in metadata if i.value != index]:
            value = row[additional.value]
            match additional:
                case MetadataField.EXTERNAL_LINKS:
                    anno[entity][idx][additional.value] = json.loads(value) if value is not None else None
                case _:
                    anno[entity][idx][additional.value] = value
