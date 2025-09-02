"""
Base class for data curation manipulation.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-09-01 by Parker Hicks
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import polars as pl

    from metahq.util.alltypes import IdArray


class BaseCuration(ABC):
    """Base abstract class for Curation children."""

    @abstractmethod
    def filter(self, condition: pl.Expr) -> BaseCuration:
        """Filters the data based on provided conditions."""

    @abstractmethod
    def select(self, *args, **kwargs) -> BaseCuration:
        """Selects specific columns from the data."""

    @abstractmethod
    def to_numpy(self) -> np.ndarray:
        """Converts the data to a numpy array."""

    @property
    @abstractmethod
    def entities(self) -> IdArray:
        """Returns array of entity column names, excluding 'index' and 'group'."""

    @property
    @abstractmethod
    def n_indices(self) -> int:
        """Returns the number of rows in the data."""

    @property
    @abstractmethod
    def n_entities(self) -> int:
        """Returns the number of entity columns."""
