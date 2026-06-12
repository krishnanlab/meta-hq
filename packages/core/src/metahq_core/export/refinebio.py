"""
Generates a refine.bio dataset from samples and series returned from a MetaHQ query.
"""

from pathlib import Path

import requests

from metahq_core.logger import setup_logger
from metahq_core.util.supported import DEFAULT_LOG_DIR

API_DATASET_URL = "https://api.refine.bio/v1/dataset/"

DATA_CART_URL = "https://www.refine.bio/dataset/"


class RefineBioExporter:
    def __init__(self):
        pass


class DatasetCreater:
    """Tools to create a pre-populated refine.bio dataset.

    Attributes:
        data (dict[str, list[str]]):
            Dictionary of experiment -> sample IDs (e.g., {SRPxxx1: [SRRxxx1, SRRxxx2, ...]}).
    """

    def __init__(self, data, loglevel: int = 20, logdir: Path | str = DEFAULT_LOG_DIR):

        self.data: dict[str, list[str]] = data
        self.logger = setup_logger(__name__, level=loglevel, log_dir=logdir)

    def post_dataset(self):
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

    def create(self):
        """Creates a refine.bio dataset"""
        result = self.post_dataset()
        self.logger.info("dataset: %s", result)
        self.logger.info(
            "populated data cart available at %s", DATA_CART_URL + result["id"]
        )
