"""
Filters and formats external links into the final format for the MetaHQ data package.
"""

import json
from pathlib import Path

import bson
import duckdb
import polars as pl

from metahq_build.config.config import (
    ACCESSIONS_KEY,
    BGEE_EXTERNAL_LINKS,
    DISIGN_ATLAS_EXTERNAL_LINKS,
    GEMMA_EXTERNAL_LINKS,
    OMICIDX_DB,
    PROCESSED_EXTERNAL_LINKS,
    SAMPLE_COMBINED_BSON,
    SERIES_COMBINED_BSON,
    STUDY_ACCESSION_KEY,
)
from metahq_build.util.logging import setup_logger


class ExternalLinkBuilder:
    """
    Builder for external links from series in MetaHQ to their entries in their
    respective web servers for relevant contributing sources.
    """

    def __init__(self):

        self.all_links = {}
        self.logger = setup_logger(__name__)

    def build(
        self,
        sample_db_path: Path = SAMPLE_COMBINED_BSON,
        series_db_path: Path = SERIES_COMBINED_BSON,
        omicidx_path: Path = OMICIDX_DB,
    ) -> "ExternalLinkBuilder":
        """Builds and combines external links across sources into a JSON string.

        Below is an example entry:

        "'GSE99039': {'DiSignAtlas': {'records': [{'id': 'DSA04920',
                                           'url': 'http://www.inbirg.com/disignatlas/detail/DSA04920'},
                                          {'id': 'DSA04922',
                                           'url': 'http://www.inbirg.com/disignatlas/detail/DSA04922'}]},
                     'Gemma': {'browse_url': 'https://gemma.msl.ubc.ca/browse/#/q/GSE99039',
                               'records': [{'id': 18776,
                                            'url': 'https://gemma.msl.ubc.ca/expressionExperiment/showExpressionExperiment.html?id=18776'}]}},

        Returns: (ExternalLinkBuilder): Returns self for chaining.

        """

        bgee = self._map_bgee(self._load_json(BGEE_EXTERNAL_LINKS), omicidx_path)
        disign_atlas = self._load_json(DISIGN_ATLAS_EXTERNAL_LINKS)
        gemma = self._load_json(GEMMA_EXTERNAL_LINKS)

        # add source links to full collection
        self._add_links(bgee, "BGee")
        self._add_links(disign_atlas, "DiSignAtlas")
        self._add_links(gemma, "Gemma")

        # remove for studies not in MetaHQ
        metahq_series = self._get_all_metahq_series(sample_db_path, series_db_path)
        self.all_links = {
            series: links
            for series, links in self.all_links.items()
            if series in metahq_series
        }

        return self

    def save(self, outfile: Path = PROCESSED_EXTERNAL_LINKS):
        """Saves the harmonized external IDs to parquet."""
        serialized_links = self.serialize()
        df = pl.LazyFrame(
            {
                "series": list(serialized_links.keys()),
                "external_links": list(serialized_links.values()),
            }
        )
        df.sink_parquet(outfile, engine="streaming")

    def serialize(self) -> dict[str, str]:
        """Serializes links for each series ID."""
        return {study: json.dumps(links) for study, links in self.all_links.items()}

    def _add_links(self, source_links: dict, source_name: str):
        """Adds links from a particular source to the full collection."""
        for study, links in source_links.items():
            self.all_links.setdefault(study, {})
            self.all_links[study][source_name] = links

    def _get_all_metahq_series(
        self, sample_db_path: Path, series_db_path: Path
    ) -> set[str]:
        """Collects all series IDs from the MetaHQ sample and series BSON databases."""
        sample_db = self._load_bson(sample_db_path)
        sample_db_series = {
            entry[ACCESSIONS_KEY][STUDY_ACCESSION_KEY] for entry in sample_db.values()
        }
        series_db_series = set(self._load_bson(series_db_path).keys())

        return sample_db_series | series_db_series

    def _map_bgee(self, data: dict, omicidx_path: Path):
        """
        Map Bgee external links. Since these are SRA-forward, they must be converted to
        GEO series IDs.
        """
        with duckdb.connect(omicidx_path, read_only=True) as conn:
            mapping = (
                conn.execute(
                    """
                WITH mapping AS
                    (SELECT accession, trim(unnest(sra_studies), '""') as sra FROM src_geo_series)
                SELECT * FROM mapping WHERE sra = ANY($1)
                """,
                    [list(data.keys())],
                )
                .pl()
                .select(["sra", "accession"])
            )
            mapping = dict(mapping.iter_rows())

        # map bgee study IDs to GEO
        data = {
            mapping[sra_study]: links
            for sra_study, links in data.items()
            if sra_study in mapping
        }

        return data

    @staticmethod
    def _load_bson(file: Path) -> dict:
        with open(file, "rb") as f:
            return bson.decode(f.read())

    @staticmethod
    def _load_json(file: Path) -> dict:
        with open(file, "r", encoding="utf-8") as f:
            return json.load(f)
