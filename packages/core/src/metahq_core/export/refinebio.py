"""
Merges refine.bio accession IDs into MetaHQ exports and creates pre-populated
refine.bio datasets from samples and series returned from a MetaHQ query.

Author: Parker Hicks
Date: 2026-06-19

Last updated: 2026-06-19 by Parker Hicks
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import requests

from metahq_core.logger import setup_logger
from metahq_core.util.supported import get_default_log_dir, refinebio_metadata

if TYPE_CHECKING:
    import logging

    from metahq_core.curations.annotations import Annotations
    from metahq_core.curations.labels import Labels

API_DATASET_URL = "https://api.refine.bio/v1/dataset/"

DATA_CART_URL = "https://www.refine.bio/dataset/"

# Maps a curation's index column to the corresponding GEO column in the
# refine.bio ID mapping table.
GEO_COL_BY_INDEX = {"sample": "gsm", "series": "gse"}


class RefineBioExporter:
    """Merges refine.bio accession IDs into MetaHQ curations and creates
    pre-populated refine.bio datasets from query results.

    Attributes:
        logger (logging.Logger):
            Python builtin Logger.

        verbose (bool):
            Controls logging outputs.
    """

    def __init__(
        self,
        logger=None,
        loglevel: int = 20,
        logdir: Path | str = get_default_log_dir(),
        verbose: bool = True,
    ):
        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose
        self._map: pl.DataFrame | None = None

    def get_refinebio(
        self, curation: Annotations | Labels, fields: list[str]
    ) -> Annotations | Labels:
        """Merge refine.bio accession IDs into a curation's IDs.

        Samples are mapped from the curation's 'sample' column (if present)
        and experiments are mapped from its 'series' column (if present),
        each via an independent left join against the refine.bio ID mapping
        table. This means a sample missing from refine.bio's sample map can
        still pick up its experiment ID through its series.

        Arguments:
            curation (Annotations | Labels):
                A curation containing samples or series matching user-specified filters.

            fields (list[str]):
                refine.bio ID fields to merge (i.e., refinebio_sample, refinebio_experiment).

        Returns:
            A new curation with merged refine.bio IDs.
        """
        ids = curation.ids
        merged = ids

        if "refinebio_sample" in fields and "sample" in ids.columns:
            sample_map = (
                self._load_map()
                .select(["gsm", "refinebio_sample"])
                .unique(subset=["gsm"])
                .rename({"gsm": "sample"})
            )
            merged = merged.join(
                sample_map, on="sample", how="left", maintain_order="left"
            )

        if "refinebio_experiment" in fields and "series" in ids.columns:
            series_map = (
                self._load_map()
                .select(["gse", "refinebio_experiment"])
                .unique(subset=["gse"])
                .rename({"gse": "series"})
            )
            merged = merged.join(
                series_map, on="series", how="left", maintain_order="left"
            )

        refinebio_cols = [col for col in fields if col in merged.columns]
        new_ids = merged.select([curation.index_col, *refinebio_cols]).fill_null("NA")

        return curation.add_ids(new_ids)

    def create_dataset(self, curation: Annotations | Labels) -> dict:
        """Create a pre-populated refine.bio dataset from a curation's samples
        and series, and submit it through refine.bio's dataset API.

        Arguments:
            curation (Annotations | Labels):
                A populated curation containing samples or series matching
                user-specified filters.

        Returns:
            The JSON response from refine.bio's dataset API.
        """
        data = self._to_experiment_samples(curation)
        return DatasetCreator(data, logger=self.log, verbose=self.verbose).create()

    def _to_experiment_samples(
        self, curation: Annotations | Labels
    ) -> dict[str, list[str]]:
        """Group a curation's refine.bio sample IDs by experiment."""
        geo_col = self._geo_col(curation.index_col)

        grouped = (
            self._load_map()
            .filter(pl.col(geo_col).is_in(curation.index))
            .group_by("refinebio_experiment")
            .agg(pl.col("refinebio_sample").unique())
        )

        return dict(
            zip(
                grouped["refinebio_experiment"].to_list(),
                grouped["refinebio_sample"].to_list(),
            )
        )

    def _geo_col(self, index_col: str) -> str:
        """Returns the refine.bio map's GEO column matching a curation's index."""
        if index_col in GEO_COL_BY_INDEX:
            return GEO_COL_BY_INDEX[index_col]

        msg = (
            "Expected curation index_col in %s, got %s.",
            list(GEO_COL_BY_INDEX),
            index_col,
        )
        if self.verbose:
            self.log.error(msg)
        raise ValueError(msg)

    def _load_map(self) -> pl.DataFrame:
        """Lazily loads and caches the refine.bio ID mapping table."""
        if self._map is None:
            self._map = pl.read_parquet(refinebio_metadata())
        return self._map


class DatasetCreator:
    """Tools to create a pre-populated refine.bio dataset.

    Attributes:
        data (dict[str, list[str]]):
            Dictionary of experiment -> sample IDs (e.g., {SRPxxx1: [SRRxxx1, SRRxxx2, ...]}).
    """

    def __init__(
        self,
        data: dict[str, list[str]],
        logger=None,
        loglevel: int = 20,
        logdir: Path | str = get_default_log_dir(),
        verbose: bool = True,
    ):
        self.data: dict[str, list[str]] = data

        if logger is None:
            logger = setup_logger(__name__, level=loglevel, log_dir=logdir)
        self.log: logging.Logger = logger
        self.verbose: bool = verbose

    def post_dataset(self) -> dict:
        """Initialize a datacart on refine.bio."""
        response = requests.post(
            API_DATASET_URL,
            json={
                "data": self.data,
                "email_ccdl_ok": False,
                "notify_me": False,
            },
        )
        response.raise_for_status()
        return response.json()

    def create(self) -> dict:
        """Creates a refine.bio dataset."""
        result = self.post_dataset()
        if self.verbose:
            self.log.info("dataset: %s", result)
            self.log.info(
                "populated data cart available at %s", DATA_CART_URL + result["id"]
            )
        return result
