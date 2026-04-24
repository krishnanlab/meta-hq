"""
Base class for OmicIDX metadata retrieval.
"""

import polars as pl

from metahq_setup.util.logging import setup_logger


class BaseMetadataRetriever:
    """Base class for metadata retrieval from OmicIDX."""

    def __init__(self):
        self.metadata = pl.DataFrame()
        self.logger = setup_logger(f"metahq_setup.metadata.{self.__class__.__name__}")
