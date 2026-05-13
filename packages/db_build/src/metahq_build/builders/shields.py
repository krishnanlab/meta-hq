"""
Build JSON endpoints for shields.io to display sample and study annotation counts per source.
"""

import json
from dataclasses import dataclass
from pathlib import Path

import bson

from metahq_build.__version__ import __version__
from metahq_build.config import (
    ATTRIBUTE_KEYS,
    SAMPLE_COMBINED_BSON,
    SERIES_COMBINED_BSON,
    SOURCE_COUNT_SHIELD_OUTDIR,
)
from metahq_build.util.logging import setup_logger


@dataclass
class EndpointParams:
    """Storage of shields.io JSON endpoints."""

    label: str
    message: str
    filename: str
    color: str = "lightgray"
    label_color: str = "gray"
    style: str = "flat"
    schema_version: int = 1

    def to_json(self):
        """Format endpoint parameters to JSON compatible with shields.io."""
        return {
            "schemaVersion": self.schema_version,
            "label": self.label,
            "message": self.message,
            "color": self.color,
            "labelColor": self.label_color,
            "style": self.style,
        }


class ShieldEndpointBuilder:
    """Build all endpoints across sources for a given level."""

    def __init__(
        self,
        sample_db: Path = SAMPLE_COMBINED_BSON,
        series_db: Path = SERIES_COMBINED_BSON,
    ):
        self.sample_db: dict = self._load_db(sample_db)
        self.series_db: dict = self._load_db(series_db)

        self._endpoints: list[EndpointParams] = []
        self.logger = setup_logger("metahq_build.builders.shields")

    def build(self) -> "ShieldEndpointBuilder":
        """Build shield.io JSON endpoints to display sample and series annotation source counts.

        Returns:
            (EndpointBuilder): Return self for chaining.
        """
        self.logger.info("Gathering source counts...")
        source_counts, all_sources = self.get_source_counts()

        self.logger.info("Found %d sources in total: %s", len(all_sources), all_sources)

        self.logger.info(
            "Constructing shields.io JSON endpoints for annotation sources..."
        )
        for level, counts in source_counts.items():
            for source, count in counts.items():
                source_endpoint = EndpointParams(
                    label=f"{level.capitalize()} annotations in MetaHQ",
                    message=format(count, ","),
                    filename=f"{source}__{level}.json",
                )
                self._endpoints.append(source_endpoint)

        # build version shield
        self.logger.info("Constructing shields.io JSON endpoint for package version...")
        version_endpoint = EndpointParams(
            label="build",
            message=f"v{__version__}",
            filename="metahq_build__version.json",
        )
        self._endpoints.append(version_endpoint)

        return self

    def save(self, outdir: Path = SOURCE_COUNT_SHIELD_OUTDIR):
        """Save all built endpoints to a single directory."""
        if not outdir.exists():
            outdir.mkdir(exist_ok=True, parents=True)

        if len(self.endpoints) == 0:
            self.logger.warning("There are no build endpoints to save.")

        self.logger.info("Saving %d JSON endpoints to %s", len(self.endpoints), outdir)
        for endpoint in self.endpoints:
            with open(outdir / endpoint.filename, "w", encoding="utf-8") as fp:
                json.dump(endpoint.to_json(), fp, indent=4)
        self.logger.info("Done!")

    def _load_db(self, file: Path) -> dict:
        """Load a MetaHQ database."""
        with open(file, "rb") as f:
            return bson.decode(f.read())

    def get_source_counts(self) -> tuple[dict[str, dict[str, int]], list[str]]:
        """Get counts for each annotation source in the sample and series MetaHQ databases."""
        counts: dict[str, dict[str, int]] = {
            "sample": self._source_counts(self.sample_db),
            "series": self._source_counts(self.series_db),
        }

        # find all possible annotation sources and add zeros to missing sources
        all_sources: list[str] = sorted(
            set(counts["sample"].keys()) | set(counts["series"].keys())
        )
        for level_counts in counts.values():
            for source in all_sources:
                if source not in level_counts:
                    level_counts[source] = 0

        return counts, all_sources

    @staticmethod
    def _source_counts(db: dict) -> dict[str, int]:
        """Count number of annotations per source."""
        source_counts: dict[str, int] = {}
        for anno in db.values():
            for attribute in ATTRIBUTE_KEYS:
                if attribute not in anno:
                    continue

                for source in anno[attribute]:
                    source_counts.setdefault(source, 0)
                    source_counts[source] += 1

        return source_counts

    @property
    def endpoints(self):
        """Return endpoints configs."""
        return self._endpoints
