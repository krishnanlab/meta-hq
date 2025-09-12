"""
Abstract base class for Curation export io classes.

Author: Parker Hicks
Date: 2025-09-08

Last updated: 2025-09-08 by Parker Hicks
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from metahq_core.curations.base import BaseCuration
    from metahq_core.util.alltypes import FilePath, NpIntMatrix


class BaseExporter(ABC):
    """Base abstract class for Exporter children."""

    @abstractmethod
    def to_json(
        self,
        curation: BaseCuration,
        file: FilePath,
        metadata: str | None,
        *args,
        **kwargs,
    ):
        """Saves curation as json."""

    @abstractmethod
    def to_numpy(self, curation: BaseCuration) -> NpIntMatrix:
        """Returns curations matrix as numpy array."""

    @abstractmethod
    def to_parquet(
        self, curation: BaseCuration, file: FilePath, metadata: str | None, **kwargs
    ):
        """Saves curation to parquet."""

    @abstractmethod
    def to_csv(
        self,
        curation: BaseCuration,
        file: FilePath,
        metadata: str | None,
        **kwargs,
    ):
        """Saves curation to csv."""

    @abstractmethod
    def to_tsv(
        self,
        curation: BaseCuration,
        file: FilePath,
        metadata: str | None,
        **kwargs,
    ):
        """Saves curation to tsv."""
