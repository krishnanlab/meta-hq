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

from metahq_build.config.config import GEMMA_RAW, GEMMA_SAMPLES_RAW
from metahq_build.util.logging import setup_logger


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
    SAMPLES_URL = "https://gemma.msl.ubc.ca/rest/v2/datasets/{id}/samples"
    QUERY_LIMIT = 100  # Per Gemma API documentation

    def __init__(self):
        self.logger = setup_logger("metahq_build.fetchers.gemma")

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

    def fetch_samples(
        self,
        input_path: Path = GEMMA_RAW,
        output_path: Path = GEMMA_SAMPLES_RAW,
    ) -> Path:
        """
        Download per-sample characteristics for datasets in a raw Gemma file.

        The ``characteristics`` field returned per-study by ``fetch()`` only
        includes annotations curators attached directly to the experiment
        record. Annotations attached to individual samples (bioAssays) or to
        experimental factor values -- which is where most sex, tissue,
        disease, and age annotations actually live in Gemma -- are not
        included there. This method fetches those separately, one request
        per dataset, and writes the combined result to a single JSON file
        keyed by Gemma dataset ID, where each value is a list of records
        with keys ``dataset_id``, ``gsm``, ``category``, ``value``, and
        ``valueUri`` -- one per distinct characteristic on each sample
        (GSM) in that dataset -- so annotations can be traced back to the
        specific sample and dataset they came from.

        Arguments:
            input_path (Path):
                Path to the raw JSON file produced by ``fetch()``. Used to
                determine which dataset IDs to fetch samples for.
            output_path (Path):
                Destination file path for the combined JSON output.

        Returns:
            (Path): Path to the saved JSON file.

        Raises:
            FileNotFoundError: If input_path does not exist.
        """
        input_path = Path(input_path)
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if not input_path.exists():
            raise FileNotFoundError(
                f"Raw Gemma data not found at {input_path}. Run fetch() first."
            )

        with open(input_path) as f:
            raw_data = json.load(f)

        dataset_ids: list[int] = []
        for batch_data in raw_data.values():
            if not isinstance(batch_data, list):
                continue
            for study in batch_data:
                if isinstance(study, dict) and study.get("id"):
                    dataset_ids.append(study["id"])

        self.logger.info(
            "Fetching per-sample characteristics for %d datasets...",
            len(dataset_ids),
        )

        sample_characteristics: dict[str, list[dict]] = {}
        failures = 0
        for dataset_id in tqdm(dataset_ids, desc="Sample characteristics"):
            try:
                characteristics = self._fetch_sample_characteristics(dataset_id)
            except requests.RequestException as e:
                failures += 1
                self.logger.warning(
                    "Failed to fetch samples for dataset %s: %s", dataset_id, e
                )
                continue

            if characteristics:
                sample_characteristics[str(dataset_id)] = characteristics

        with open(output_path, "w") as f:
            json.dump(sample_characteristics, f, indent=4)

        self.logger.info(
            "Saved sample-level characteristics for %d datasets to %s (%d failed)",
            len(sample_characteristics),
            output_path,
            failures,
        )
        return output_path

    def _fetch_sample_characteristics(self, dataset_id: int) -> list[dict]:
        """
        Fetch characteristics attached to a dataset's individual samples.

        Pulls each sample's own ``characteristics`` as well as the
        ``characteristics`` nested under each of its ``factorValueObjects``,
        since Gemma stores per-sample and experimental-factor annotations
        separately from the dataset-level characteristics list. The Gemma
        API returns all samples for a dataset in a single response
        regardless of ``limit``/``offset``, so no pagination is needed here.

        Arguments:
            dataset_id (int):
                Gemma dataset ID.

        Returns:
            (list[dict]): One record per distinct (GSM, category, value,
                valueUri) combination found across the dataset's samples,
                each with keys ``dataset_id``, ``gsm``, ``category``,
                ``value``, and ``valueUri``.

        Raises:
            requests.RequestException: If the request fails or returns a
                non-2xx status.
        """
        url = self.SAMPLES_URL.format(id=dataset_id)
        response = requests.get(
            url,
            headers={"Accept": "application/json", "Accept-Encoding": "gzip"},
            timeout=30,
        )
        response.raise_for_status()

        seen: set[tuple] = set()
        characteristics: list[dict] = []
        for row in response.json().get("data", []):
            gsm = (row.get("accession") or {}).get("accession")
            sample = row.get("sample") or {}
            char_lists = [sample.get("characteristics") or []]
            for factor_value in sample.get("factorValueObjects") or []:
                char_lists.append(factor_value.get("characteristics") or [])

            for char_list in char_lists:
                for char in char_list:
                    if not isinstance(char, dict):
                        continue
                    key = (
                        gsm,
                        char.get("category"),
                        char.get("value"),
                        char.get("valueUri"),
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    characteristics.append(
                        {
                            "dataset_id": dataset_id,
                            "gsm": gsm,
                            "category": char.get("category"),
                            "value": char.get("value"),
                            "valueUri": char.get("valueUri"),
                        }
                    )

        return characteristics
