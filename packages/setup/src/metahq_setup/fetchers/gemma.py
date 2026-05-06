"""
Fetcher for Gemma database annotations.

Downloads study annotations from the Gemma REST API in batches and saves
them to a single combined JSON file.
"""

import json
import shutil
from pathlib import Path

import requests
from tqdm import tqdm

from metahq_setup.config.config import GEMMA_RAW
from metahq_setup.util.logging import setup_logger


class GemmaFetcher:
    """
    Downloads study annotations from the Gemma REST API.

    Fetches in batches of QUERY_LIMIT studies, saves each batch to a
    temporary directory, then combines them into a single JSON file with
    the structure ``{batch_index: [study, ...]}``.

    Attributes:
        BASE_URL (str):
            Gemma v2 REST API endpoint for dataset queries.
        QUERY_LIMIT (int):
            Number of studies per API request (per Gemma API documentation).
    """

    BASE_URL = "https://gemma.msl.ubc.ca/rest/v2/datasets"
    QUERY_LIMIT = 100  # Per Gemma API documentation

    def __init__(self):
        self.logger = setup_logger("metahq_setup.fetchers.gemma")

    def fetch(
        self,
        output_path: Path = GEMMA_RAW,
        query: str = "sort=-id",
        max_studies: int = 60_000,
    ) -> Path:
        """
        Download Gemma annotations and save to a JSON file.

        Fetches studies in batches of QUERY_LIMIT, writes each batch to a
        temporary directory, then combines all non-empty batches into a
        single JSON file at output_path. The temporary directory is always
        cleaned up, even on failure.

        Arguments:
            output_path (Path):
                Destination file path for the combined JSON output.
                Defaults to the package-wide GEMMA_RAW constant.
            query (str):
                Gemma API query string appended to the base URL
                (e.g. ``"sort=-id"``).
            max_studies (int):
                Upper bound on the number of studies to download.

        Returns:
            (Path): Path to the saved JSON file.

        Raises:
            requests.HTTPError: If any batch request returns a non-2xx status.
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        temp_dir = output_path.parent / "_gemma_tmp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            offsets = range(0, max_studies, self.QUERY_LIMIT)
            self.logger.info(
                "Downloading up to %d studies from Gemma in %d batches...",
                max_studies,
                len(offsets),
            )

            for offset in tqdm(offsets, desc="Batch download", total=len(offsets)):
                batch_file = temp_dir / f"gemma_{offset:06d}.json"
                self._fetch_batch(query, offset, batch_file)

            self.logger.info("Combining batch files...")
            annotations: dict[str, list] = {}
            for idx, batch_file in enumerate(sorted(temp_dir.glob("gemma_*.json"))):
                with open(batch_file) as f:
                    data = json.load(f)
                if data.get("data"):
                    annotations[str(idx)] = data["data"]

            with open(output_path, "w") as f:
                json.dump(annotations, f, indent=4)

            self.logger.info(
                "Saved %d non-empty batches to %s", len(annotations), output_path
            )
            return output_path

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _fetch_batch(self, query: str, offset: int, output_file: Path) -> None:
        """
        Fetch a single batch of studies from the Gemma API.

        Arguments:
            query (str):
                Gemma API query string.
            offset (int):
                Pagination offset for this batch.
            output_file (Path):
                Destination path for the raw JSON response.

        Raises:
            requests.HTTPError: If the request returns a non-2xx status.
        """
        url = f"{self.BASE_URL}?{query}&offset={offset}&limit={self.QUERY_LIMIT}"
        response = requests.get(
            url,
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
            timeout=30,
        )
        response.raise_for_status()

        with open(output_file, "w") as f:
            json.dump(response.json(), f)
