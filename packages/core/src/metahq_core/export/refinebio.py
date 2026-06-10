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
    def __init__(self, loglevel: int = 20, logdir: Path | str = DEFAULT_LOG_DIR):
        self.logger = setup_logger(__name__, level=loglevel, log_dir=logdir)

    def _post_dataset(self, data):
        """Initialize a datacart on refine.bio.

        Arguments:
            data (dict[str, list[str]]):
                Dictionary of experiment -> sample IDs (e.g., {SRPxxx1: [SRRxxx1, SRRxxx2, ...]}).
            token (str):
                refine.bio API token.

        """
        response = requests.post(
            API_DATASET_URL,
            json={
                "data": data,
                "email_ccdl_ok": False,
                "notify_me": False,
            },
        )
        response.raise_for_status()
        return response.json()

    def create(self):
        """Creates a refine.bio dataset"""
        result = self._post_dataset(data)
        print("dataset:", result)
        print("")
        print(f"populated data cart available at {DATA_CART_URL}{result['id']}")
