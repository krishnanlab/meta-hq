"""
Class for Labels export io classes.

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
from metahq_core.util.supported import database_ids, disease_ontologies, get_default_log_dir

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.labels import Labels
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


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

    def __init__(
        self,
        attribute: Literal["tissue", "disease", "sex", "age"],
        level: Literal["sample", "series"],
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

    def get_sra(self, labels: Labels, fields: list[str]) -> Labels:
        """Retrieve SRA IDs from the annotations if they exist.

        Arguments:
            labels (Labels):
                A Labels curation containing samples and terms matching user-specified
                filters.

            fields (list[str]):
                SRA ID levels (i.e., srr, srx, srs, or srp)

        Returns:
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
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save labels curation to json. Keys are terms and values are
        positively, negative, netral, and control labeled entries.

        Arguments:
            labels (Labels):
                A populated Labels curation object.

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
        opt[fmt](labels, file, citation_config, metadata, **kwargs)

        if self.verbose:
            self.log.info("Saved!")

    def to_refinebio_dataset(self, curation: Labels) -> dict:
        """Create a pre-populated refine.bio dataset from this curation's
        samples and series, and submit it through refine.bio's dataset API.

        Arguments:
            curation (Labels):
                A populated Labels curation.

        Returns:
            The JSON response from refine.bio's dataset API.
        """
        return self._refinebio.create_dataset(curation)

    def to_csv(
        self,
        curation: Labels,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save labels to csv.

        Arguments:
            curation (Labels):
                A populated Labels curation object.

            file (FilePath):
                Path to outfile.csv.

            metadata (str):
                Metadata fields to include.

        """
        self._save_tabular("csv", curation, file, citation_config, metadata, **kwargs)

    def to_json(
        self,
        curation: Labels,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
    ):
        """Save labels curation to json. Keys are terms and values are
        positively labelstated indices.

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

        if (metadata is None) or (
            isinstance(metadata, str)
            & (metadata.strip().replace(",", "") == curation.index_col)
        ):
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
            _metadata.extend([SOURCES_COL])

            if self._sra_in_metadata(_metadata):
                curation = self.get_sra(
                    curation,
                    [field for field in _metadata if field in database_ids("sra")],
                )

            if self._refinebio_in_metadata(_metadata):
                curation = self._refinebio.get_refinebio(
                    curation,
                    [
                        field
                        for field in _metadata
                        if field in database_ids("refinebio")
                    ],
                )

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
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save labels to parquet.

        Arguments:
            curation (Labels):
                Labels curation object to save.

            file (FilePath):
                Path to outfile.parquet.

            metadata (str | None):
                Metadata fields to include.

        """
        self._save_tabular(
            "parquet", curation, file, citation_config, metadata, **kwargs
        )

    def to_tsv(
        self,
        curation: Labels,
        file: FilePath,
        citation_config: CitationConfig,
        metadata: str | None = None,
        **kwargs,
    ):
        """Save labels to tsv.

        Arguments:
            curation (Labels):
                A populated Labels curation object.

            file (FilePath):
                Path to outfile.tsv.

            metadata (str):
                Metadata fields to include.

        """
        self._save_tabular("tsv", curation, file, citation_config, metadata, **kwargs)

    def _save_tabular(
        self,
        fmt: str,
        curation: Labels,
        file: FilePath,
        citation_config: CitationConfig,
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

        if self._refinebio_in_metadata(_metadata):
            curation = self._refinebio.get_refinebio(
                curation,
                [field for field in _metadata if field in database_ids("refinebio")],
            )

        _metadata = _metadata + [SOURCES_COL]

        # save sources to citation file
        save_citations(
            curation.ids[SOURCES_COL].str.split("|").explode().value_counts(sort=True),
            citation_config,
            logger=self.log,
            verbose=self.verbose,
        )

        self.log.info("Saving retrieval result to %s", Path(file).parent)
        if self._geo_fields_in_metadata(_metadata, curation.index_col):
            self._save_table_with_geo_metadata(
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
