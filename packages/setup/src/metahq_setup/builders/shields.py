"""
Build JSON endpoints for shields.io to display sample and study annotation counts per source.
"""

from pathlib import Path
from dataclasses import dataclass
from typing import Literal

import bson 

from metahq_setup.processors import ProcessorRegistry
from metahq_setup.util.logging import setup_logger

@dataclass
class EndpointParams:
    """Storage of shields.io JSON endpoints."""
    label: str
    message: str
    outfile: Path
    color: str = "lightgray"
    label_color: str = "gray"
    style: str = "flat"
    schema_version: int = 1


class EndpointBuilder:
    """Build all endpoints across sources for a given level."""
    def __init__(self, database: Path, level: Literal["sample", "series"]):
        self.db = self._load_db(database)
        self.level = level
        self._endpoints: list[EndpointParams] = []
        self.logger = setup_logger("metahq_setup.builders.shields")

    def build(self, ):
        pass

    def save(self):
        pass

    def _load_db(self, file: Path) -> dict:
        """Load a MetaHQ database."""
        with open(file, "rb", encoding="utf-8") as f:
            return bson.decode(f.read())

    def _source_counts(self):
        """Count number of annotations per source."""
