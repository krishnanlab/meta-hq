"""
Base class for data curation manipulation.

Author: Parker Hicks
Date: 2025-08-13

Last updated: 2025-09-01 by Parker Hicks
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl

    from metahq_core.util.alltypes import IdArray


class BaseCuration(ABC):
    """Base abstract class for Curation children."""

    @abstractmethod
    def add_ids(self, new: pl.DataFrame) -> BaseCuration:
        """Joins additional ID columns with the curation IDs. Joins on the curation index."""

    @abstractmethod
    def add_ids_on_group(self, new: pl.DataFrame, on: str | list[str]) -> BaseCuration:
        """Joins additional ID columns with the curation IDs, but joins on a
        set of group IDs rather than the index.
        """

    @abstractmethod
    def add_ids_partial(self, new: pl.DataFrame) -> BaseCuration:
        """Appends new group ID columns to the IDs of an BaseCuration object. IDs in the new data
        frame that are not in the original will be dropped.

        Arguments:
            new (pl.DataFrame):
                A DataFrame of additional IDs to join with the current index column of `data`.

        Returns:
            A new BaseCuration object including the new ID columns.
        """

    @abstractmethod
    def filter(self, condition: pl.Expr) -> BaseCuration:
        """Filters the data based on provided conditions."""

    @abstractmethod
    def head(self, *args, **kwargs) -> str:
        """Wrapper for polars `head`."""

    @abstractmethod
    def select(self, *args, **kwargs) -> BaseCuration:
        """Selects specific columns from the data."""

    @abstractmethod
    def slice(self, offset: int, length: int | None = None) -> BaseCuration:
        """Slice both data and ids simultaneously using polars slice."""

    @abstractmethod
    def pl(self) -> pl.DataFrame:
        """Return a curation as a polars dataframe."""

    @property
    @abstractmethod
    def entities(self) -> IdArray:
        """Returns array of entity column names, excluding 'index' and 'group'."""

    @property
    @abstractmethod
    def groups(self) -> list[str]:
        """Returns the groups column of the Annotations curation."""

    @property
    @abstractmethod
    def ids(self) -> pl.DataFrame:
        """Return the IDs dataframe."""

    @property
    @abstractmethod
    def index(self) -> list:
        """Return the index column as a list."""

    @property
    @abstractmethod
    def n_indices(self) -> int:
        """Returns the number of rows in the data."""

    @property
    @abstractmethod
    def n_entities(self) -> int:
        """Returns the number of entity columns."""

    @property
    @abstractmethod
    def unique_groups(self) -> list[str]:
        """Returns unique groups."""
