"""
CREEDS perturbation annotation processor.

Processes annotations from CREEDS (CRowd Extracted Expression of Differential Signatures),
which provides crowd-sourced disease perturbation annotations.
"""

import json
from pathlib import Path

import polars as pl

from metahq_setup.config.config import (
    CREEDS_JSON,
    COL_ACCESSION,
    COL_ATTRIBUTE,
    COL_ECODE,
    COL_TERM_ID,
    COL_TERM_NAME,
    MONDO_OBO,
    MONDO_SYSTEMS,
)
from metahq_setup.ontology import Ontology, get_system_descendants
from metahq_setup.processors.base import BaseProcessor
from metahq_setup.processors.registry import ProcessorRegistry


@ProcessorRegistry.register
class CREEDSProcessor(BaseProcessor):
    """
    Processor for CREEDS crowd-sourced disease annotations.

    CREEDS provides crowd-sourced annotations for disease perturbations
    with both control and perturbation sample annotations.
    """

    source_name = "creeds"
    version = "1.0.0"
    description = "CREEDS crowd-sourced perturbation annotations"

    def process(self, output_dir: Path, **kwargs) -> pl.DataFrame:
        """Process CREEDS annotations into standardized format.

        Arguments:
            output_dir (Path):
                Directory where the processed parquet file will be written.
            **kwargs:
                ``input_path`` (Path) - override CREEDS JSON input file

        Returns:
            (pl.DataFrame): Standardized annotations with columns
                ``sample_id``, ``annotation_type``, ``term_id``,
                ``term_label``, and ``ecode``.
        """
        input_path = Path(kwargs.get("input_path", CREEDS_JSON))
        self.logger.info("Processing CREEDS annotations from %s", input_path)

        # Load CREEDS JSON data
        with open(input_path, "r") as f:
            creeds_data = json.load(f)

        self.logger.info("Loaded %s CREEDS signature entries", len(creeds_data))

        # Load MONDO ontology for DOID mapping
        self.logger.info("Loading MONDO ontology for DOID mapping...")
        mondo = Ontology.from_obo(MONDO_OBO)

        # Get unique DOIDs from data (filter for valid DOIDs only)
        valid_doids = set()
        for entry in creeds_data:
            if self._is_valid_entry(entry):
                doid = entry["do_id"]
                if doid and isinstance(doid, str) and doid.startswith("DOID:"):
                    valid_doids.add(doid)

        self.logger.info("Found %s unique valid DOIDs to map", len(valid_doids))

        # Map DOID to MONDO
        doid_to_mondo = mondo.map_terms(
            terms=list(valid_doids),
            ontology="MONDO",
            _from="DOID",
            _to="MONDO"
        )

        # Load MONDO system descendants for filtering
        self.logger.info("Loading MONDO system descendants for filtering...")
        valid_mondo = get_system_descendants(MONDO_SYSTEMS, MONDO_OBO)

        # Process entries and create annotation records
        records = []
        skipped_system_level = 0
        for entry in creeds_data:
            if not self._is_valid_entry(entry):
                continue

            doid = entry["do_id"]

            # Skip if DOID doesn't map to MONDO
            if doid not in doid_to_mondo:
                continue

            mondo_id = doid_to_mondo[doid]
            if mondo_id == "NA":
                continue

            # Skip if MONDO ID is at system level or higher (not in descendants)
            if mondo_id not in valid_mondo:
                skipped_system_level += 1
                continue

            disease_name = entry.get("disease_name", "unknown")

            # Process perturbation samples (disease samples)
            pert_ids = entry.get("pert_ids", [])
            for gsm_id in pert_ids:
                records.append({
                    COL_ACCESSION: gsm_id,
                    COL_ATTRIBUTE: "disease",
                    COL_TERM_ID: mondo_id,
                    COL_TERM_NAME: disease_name,
                    COL_ECODE: "crowd",
                })

            # Process control samples
            ctrl_ids = entry.get("ctrl_ids", [])
            for gsm_id in ctrl_ids:
                records.append({
                    COL_ACCESSION: gsm_id,
                    COL_ATTRIBUTE: "disease",
                    COL_TERM_ID: "MONDO:0000000",  # Control samples
                    COL_TERM_NAME: "control",
                    COL_ECODE: "crowd",
                })

        if skipped_system_level > 0:
            self.logger.info(
                "Skipped %s entries with system-level or higher MONDO terms.",
                skipped_system_level,
            )

        result_df = pl.DataFrame(records)

        self.logger.info(
            "Produced %s disease annotations from CREEDS (%s perturbation + %s control)",
            len(result_df),
            len([r for r in records if r[COL_TERM_ID] != "MONDO:0000000"]),
            len([r for r in records if r[COL_TERM_ID] == "MONDO:0000000"]),
        )

        # Save processed data
        output_file = output_dir / "creeds_processed.parquet"
        result_df.write_parquet(output_file)
        self.logger.info("Wrote processed data to %s", output_file)

        return result_df

    def _is_valid_entry(self, entry: dict) -> bool:
        """Check if CREEDS entry is valid for processing.

        Arguments:
            entry (dict):
                CREEDS signature entry.

        Returns:
            (bool): True if entry is human and has valid DOID.
        """
        # Must be human organism
        if entry.get("organism") != "human":
            return False

        # Must have a valid DOID
        do_id = entry.get("do_id")
        if not do_id or not isinstance(do_id, str):
            return False

        return True

    def validate(self, data: pl.DataFrame) -> bool:
        """Validate processed CREEDS data.

        Arguments:
            data (pl.DataFrame):
                Processed annotations DataFrame to validate.

        Returns:
            (bool): True if validation passes.

        Raises:
            ValidationError: If required columns are missing.
        """
        self._validate_required_columns(data)

        # Check that disease annotations are present
        annotation_types = data[COL_ATTRIBUTE].unique().to_list()
        if "disease" not in annotation_types:
            self.logger.warning("No disease annotations found in CREEDS output.")

        # Verify all records have ecode='crowd'
        if not all(e == "crowd" for e in data[COL_ECODE].unique().to_list()):
            self.logger.warning("Found non-crowd ecode values in CREEDS data.")

        return True
