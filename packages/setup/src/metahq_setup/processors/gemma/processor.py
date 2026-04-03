"""
Gemma database annotation processor.

Downloads and processes annotations from the Gemma database
(https://gemma.msl.ubc.ca).
"""

import json
import shutil
from pathlib import Path

import polars as pl
import requests
from tqdm import tqdm

from metahq_setup.processors.base import BaseProcessor, ProcessorError
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class GemmaProcessor(BaseProcessor):
    """
    Processor for Gemma database annotations.

    Gemma is a database of gene expression studies with curated annotations
    for tissues, diseases, and experimental conditions.
    """

    source_name = "gemma"
    version = "1.0.0"
    description = "Gemma database annotations for gene expression studies"

    QUERY_LIMIT = 100  # Per Gemma API documentation
    BASE_URL = "https://gemma.msl.ubc.ca/rest/v2/datasets"

    def download(self, output_dir: Path, max_studies: int = 21400, **kwargs) -> Path:
        """
        Download annotations from Gemma API.

        Arguments:
            output_dir (Path):
                Directory to save downloaded files
            max_studies (int):
                Maximum number of studies to download
            **kwargs:
                Additional arguments (query string for Gemma API)

        Returns:
            (Path): Path to downloaded BSON file
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        temp_dir = output_dir / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)

        query = kwargs.get("query", "sort=-id")

        try:
            self.logger.info(f"Downloading up to {max_studies} studies from Gemma...")

            # Download in batches
            offsets = range(0, max_studies, self.QUERY_LIMIT)
            for offset in tqdm(
                offsets, desc="Downloading batches", total=len(offsets)
            ):
                url = f"{self.BASE_URL}?{query}&offset={offset}&limit={self.QUERY_LIMIT}"
                output_file = temp_dir / f"gemma_{offset}.json"

                response = requests.get(
                    url,
                    headers={"accept": "application/json"},
                    timeout=30,
                )
                response.raise_for_status()

                with open(output_file, "w") as f:
                    json.dump(response.json(), f)

            # Combine all downloaded files
            annotations = {}
            for idx, file in enumerate(sorted(temp_dir.glob("*.json"))):
                with open(file, "r") as f:
                    data = json.load(f)
                    if "data" in data:
                        annotations[str(idx)] = data["data"]

            # Save combined annotations
            raw_file = output_dir / "gemma_raw.json"
            with open(raw_file, "w") as f:
                json.dump(annotations, f)

            # Clean up temp directory
            shutil.rmtree(temp_dir)

            self.logger.info(f"Downloaded {len(annotations)} batches")
            return raw_file

        except requests.RequestException as e:
            raise ProcessorError(f"Failed to download Gemma data: {e}")

    def process(self, input_path: Path, output_dir: Path, **kwargs) -> pl.DataFrame:
        """
        Process Gemma annotations into standardized format.

        Arguments:
            input_path (Path):
                Path to raw Gemma JSON file
            output_dir (Path):
                Directory for processed output
            **kwargs:
                Additional processing arguments

        Returns:
            (pl.DataFrame): Standardized annotations
        """
        self.logger.info("Processing Gemma annotations...")

        # Load raw data
        with open(input_path, "r") as f:
            raw_data = json.load(f)

        # Process annotations
        records = []
        for batch_id, batch_data in raw_data.items():
            if not isinstance(batch_data, list):
                continue

            for study in batch_data:
                if not isinstance(study, dict):
                    continue

                # Extract study ID (typically GEO series ID)
                study_id = study.get("accession", {}).get("accession", "")
                if not study_id:
                    continue

                # Process tags/annotations
                tags = study.get("tags", [])
                for tag in tags:
                    if isinstance(tag, dict):
                        term_uri = tag.get("termUri", "")
                        term_label = tag.get("term", "")

                        if term_uri and term_label:
                            # Extract ontology term ID from URI
                            term_id = self._extract_term_id(term_uri)
                            annotation_type = self._infer_annotation_type(term_uri)

                            records.append(
                                {
                                    "sample_id": study_id,
                                    "annotation_type": annotation_type,
                                    "term_id": term_id,
                                    "term_label": term_label,
                                    "confidence": 1.0,  # Gemma annotations are curated
                                    "source": self.source_name,
                                }
                            )

        df = pl.DataFrame(records)
        self.logger.info(f"Processed {len(df)} annotations from Gemma")

        # Save processed data
        output_file = output_dir / "gemma_processed.parquet"
        df.write_parquet(output_file)

        return df

    def validate(self, data: pl.DataFrame) -> bool:
        """
        Validate processed Gemma data.

        Arguments:
            data (pl.DataFrame):
                Processed data to validate

        Returns:
            (bool): True if valid
        """
        self._validate_required_columns(data)

        # Additional Gemma-specific validation
        if len(data) == 0:
            self.logger.warning("No annotations processed from Gemma")

        return True

    def _extract_term_id(self, term_uri: str) -> str:
        """
        Extract ontology term ID from URI.

        Arguments:
            term_uri (str):
                URI like "http://purl.obolibrary.org/obo/UBERON_0000948"

        Returns:
            (str): Term ID like "UBERON:0000948"
        """
        if "/" in term_uri:
            term_part = term_uri.split("/")[-1]
            return term_part.replace("_", ":")
        return term_uri

    def _infer_annotation_type(self, term_uri: str) -> str:
        """
        Infer annotation type from ontology in URI.

        Arguments:
            term_uri (str):
                Ontology term URI

        Returns:
            (str): Annotation type (tissue, disease, cell_type, etc.)
        """
        uri_lower = term_uri.lower()

        if "uberon" in uri_lower or "bto" in uri_lower:
            return "tissue"
        elif "mondo" in uri_lower or "doid" in uri_lower:
            return "disease"
        elif "cl_" in uri_lower or "cell" in uri_lower:
            return "cell_type"
        elif "efo" in uri_lower:
            return "experimental_factor"
        else:
            return "other"
