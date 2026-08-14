"""
Class for Labels export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2026-08-14 by Parker Hicks
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from metahq_core.config import SOURCES_COL
from metahq_core.export.base import BaseExporter
from metahq_core.export.references import save_citations
from metahq_core.util.alltypes import MetadataField
from metahq_core.util.io import save_json
from metahq_core.util.supported import disease_ontologies

if TYPE_CHECKING:
    from metahq_core.curations.labels import Labels
    from metahq_core.export.references import CitationConfig
    from metahq_core.util.alltypes import FilePath


LABEL_KEY = {"1": "positive", "-1": "negative", "2": "control"}


class LabelsExporter(BaseExporter):
    """Exporter for Labels curations.

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
        curation: Labels,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
    ):
        """Save labels curation to json. Keys are terms and values are
        positive, negative, and (for disease ontology terms) control labeled
        entries.

        Arguments:
            curation (Labels):
                A populated Labels curation object.

            file (FilePath):
                Path to outfile.json.

            metadata (str):
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

        if self._only_index(metadata, curation.index_col):
            metadata = curation.index_col

        if isinstance(metadata, str):
            save_citations(
                curation.ids[SOURCES_COL]
                .str.split("|")
                .explode()
                .value_counts(sort=True),
                citation_config,
                logger=self.log,
                verbose=self.verbose,
            )

            self.log.info("Saving retrieval result to %s", Path(file).parent)
            _metadata = self._parse_metafields(curation.index_col, metadata)

            if MetadataField.EXTERNAL_LINKS in _metadata:
                curation = self.add_external_links(curation)

            curation = self._join_external_metadata_with_curation(
                curation, fields=_metadata
            )

            # append requested GEO metadata fields
            stacked = curation.data.hstack(curation.ids)
            geo_fields = self._geo_fields_in_metadata(_metadata, curation.index_col)
            if geo_fields:
                geo = self._get_geo_metadata(curation, geo_fields)
                stacked = stacked.join(geo, on=curation.index_col, how="left").sort(
                    curation.index_col
                )

            for row in stacked.iter_rows(named=True):
                self._write_row_with_metadata(
                    row, _labels, curation.index_col, _metadata
                )
        else:
            msg = f"Unexpected metadata argument: {metadata!r}"
            self.log.error(msg)
            self.log.debug("metadata dtype: %s", type(metadata))
            raise ValueError(msg)

        save_json(_labels, file)

    def _write_row_with_metadata(
        self,
        row: dict[str, str],
        labels: dict[str, dict],
        index_col: str,
        metadata: list[MetadataField],
    ):
        """Write a row of a Labels curation to a dictionary with metadata."""
        idx = row[index_col]
        for entity in labels:
            label = str(row[entity])

            if label not in LABEL_KEY:
                continue

            # add sample with metadata
            cls = LABEL_KEY[label]
            idx_metadata = {idx: {}}

            for additional in [i for i in metadata if i.value != index_col]:
                value = row[additional.value]

                # external links need to be loaded from JSON strings before
                # exporting to JSON.
                match additional:
                    case MetadataField.EXTERNAL_LINKS:
                        idx_metadata[idx][additional.value] = (
                            json.loads(value) if value is not None else None
                        )
                    case _:
                        idx_metadata[idx][additional.value] = row[additional.value]

            labels[entity][cls].append(idx_metadata)
